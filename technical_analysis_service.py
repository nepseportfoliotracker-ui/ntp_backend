# technical_analysis_service.py - Modified for 175 days

import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from scipy.signal import argrelextrema
from sklearn.cluster import KMeans

logger = logging.getLogger(__name__)


class TechnicalAnalysisService:
    """Service for calculating support and resistance levels from NEPSE historical data"""
    
    def __init__(self, nepse_history_service):
        self.nepse_history_service = nepse_history_service
        self.default_window = 5  # Sensitivity for local minima/maxima detection
        self.merge_threshold = 0.005  # 0.5% threshold for merging nearby levels
        self.max_clusters = 5  # Maximum number of support/resistance zones
        self.analysis_days = 175  # MODIFIED: Use 175 days for S/R analysis
        self.strength_threshold = 0.70  # Show levels with 70%+ strength
    
    def _prepare_dataframe(self, history_data):
        """Convert history data to pandas DataFrame"""
        if not history_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(history_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date', ascending=True)  # Sort by date ascending for analysis
        df = df.dropna(subset=['index_value'])
        df.set_index('date', inplace=True)
        
        return df
    
    def _get_data_by_days(self, days):
        """
        Get NEPSE history data for specific number of days
        
        Parameters:
        - days: Number of days (7, 30, 175, 365)
        
        Returns:
        - List of history data points
        """
        # Always fetch from yearly data and filter by days
        all_data = self.nepse_history_service.get_yearly_data()
        
        if not all_data:
            return []
        
        # Convert to DataFrame for easier date filtering
        df = pd.DataFrame(all_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date', ascending=False)  # Most recent first
        
        # Get data for specified days
        cutoff_date = datetime.now() - timedelta(days=days)
        filtered_df = df[df['date'] >= cutoff_date]
        
        # Take exact number of days if available
        filtered_df = filtered_df.head(days)
        
        return filtered_df.to_dict('records')
    
    def _detect_local_extrema(self, prices, window=5):
        """
        Detect local minima (support) and maxima (resistance) points
        Uses scipy.signal.argrelextrema
        
        Parameters:
        - prices: Array of price values
        - window: Sensitivity parameter (3-5 for balanced detection)
        
        Returns:
        - min_indices: Indices of local minima
        - max_indices: Indices of local maxima
        """
        min_idx = argrelextrema(prices, np.less_equal, order=window)[0]
        max_idx = argrelextrema(prices, np.greater_equal, order=window)[0]
        
        logger.info(f"Detected {len(min_idx)} support points and {len(max_idx)} resistance points")
        
        return min_idx, max_idx
    
    def _count_touches(self, zone_level, prices, price_range):
        """
        Count how many times price touched a zone
        
        Parameters:
        - zone_level: The price level of the zone
        - prices: Array of all prices
        - price_range: Total price range
        
        Returns:
        - Number of touches
        """
        # Count touches within 1% of price range
        touch_threshold = price_range * 0.01
        touches = np.sum(np.abs(prices - zone_level) < touch_threshold)
        return int(touches)
    
    def _merge_nearby_levels(self, levels, price_range):
        """
        Merge nearby support/resistance levels
        
        Parameters:
        - levels: List of level dictionaries with 'level' and 'touches'
        - price_range: Total price range
        
        Returns:
        - Merged list of levels with recalculated strength
        """
        if not levels:
            return []
        
        # Sort by level
        levels.sort(key=lambda x: x['level'])
        
        merged = []
        merge_threshold = 0.005  # 0.5%
        
        for level in levels:
            if not merged:
                # Calculate initial strength
                level['strength'] = min(1.0, 0.65 + (level['touches'] * 0.05))
                merged.append(level)
            else:
                last_level = merged[-1]['level']
                current_level = level['level']
                
                # Check if levels are close enough to merge
                if abs(current_level - last_level) / last_level <= merge_threshold:
                    # Merge: average level and sum touches
                    merged[-1]['level'] = (last_level + current_level) / 2
                    merged[-1]['touches'] += level['touches']
                    merged[-1]['strength'] = min(1.0, 0.65 + (merged[-1]['touches'] * 0.05))
                else:
                    # Calculate strength for new level
                    level['strength'] = min(1.0, 0.65 + (level['touches'] * 0.05))
                    merged.append(level)
        
        return merged
    
    def calculate_support_resistance(self, days=175, window=None):
        """
        Calculate support and resistance levels for given number of days
        IMPORTANT: S/R analysis always uses 175 days data regardless of display period
        
        Parameters:
        - days: Number of days for display (7, 30, 175, 365)
        - window: Sensitivity for extrema detection (default: 3)
        
        Returns:
        - Dictionary with support/resistance analysis
        """
        try:
            # ALWAYS use 175 days for S/R calculation
            analysis_data = self._get_data_by_days(self.analysis_days)
            
            if not analysis_data:
                return {'error': 'No historical data available'}
            
            # Prepare DataFrame
            df = self._prepare_dataframe(analysis_data)
            if df.empty:
                return {'error': 'Failed to prepare data'}
            
            # Use custom window or default (3 for better detection)
            window = window or 3
            
            # Detect local extrema
            prices = df['index_value'].values
            min_idx, max_idx = self._detect_local_extrema(prices, window)
            
            # Get min/max prices for calculations
            minPrice = float(df['index_value'].min())
            maxPrice = float(df['index_value'].max())
            priceRange = maxPrice - minPrice
            
            # Extract support and resistance values with their indices
            support_points = [
                {
                    'level': float(df.iloc[i]['index_value']),
                    'touches': self._count_touches(df.iloc[i]['index_value'], prices, priceRange),
                    'strength': 0.0  # Will be recalculated
                }
                for i in min_idx
            ]
            
            resistance_points = [
                {
                    'level': float(df.iloc[i]['index_value']),
                    'touches': self._count_touches(df.iloc[i]['index_value'], prices, priceRange),
                    'strength': 0.0  # Will be recalculated
                }
                for i in max_idx
            ]
            
            # Merge nearby levels
            support_points = self._merge_nearby_levels(support_points, priceRange)
            resistance_points = self._merge_nearby_levels(resistance_points, priceRange)
            
            logger.info(f"After merging: {len(support_points)} support zones, {len(resistance_points)} resistance zones")
            
            # Get latest price
            latest_price = float(df['index_value'].iloc[-1])
            latest_date = df.index[-1].strftime('%Y-%m-%d')
            
            # Classify zones as support or resistance based on current price
            support_levels = []
            resistance_levels = []
            
            for point in support_points:
                if point['level'] < latest_price:
                    support_levels.append({
                        'level': point['level'],
                        'strength': point['strength'],
                        'touches': point['touches'],
                        'distance': latest_price - point['level'],
                        'distance_percent': ((latest_price - point['level']) / latest_price) * 100
                    })
            
            for point in resistance_points:
                if point['level'] > latest_price:
                    resistance_levels.append({
                        'level': point['level'],
                        'strength': point['strength'],
                        'touches': point['touches'],
                        'distance': point['level'] - latest_price,
                        'distance_percent': ((point['level'] - latest_price) / latest_price) * 100
                    })
            
            logger.info(f"Classified: {len(support_levels)} supports below price, {len(resistance_levels)} resistances above price")
            
            # Filter to only show strong levels
            support_levels = [s for s in support_levels if s['strength'] >= self.strength_threshold]
            resistance_levels = [r for r in resistance_levels if r['strength'] >= self.strength_threshold]
            
            logger.info(f"After filtering (>={self.strength_threshold*100}%): {len(support_levels)} supports, {len(resistance_levels)} resistances")
            
            # Sort by strength descending and limit to top 5 each
            support_levels.sort(key=lambda x: x['strength'], reverse=True)
            resistance_levels.sort(key=lambda x: x['strength'], reverse=True)
            
            support_levels = support_levels[:5]  # Top 5 supports
            resistance_levels = resistance_levels[:5]  # Top 5 resistances
            
            # Get all zones for reference
            all_zones = [s['level'] for s in support_levels] + [r['level'] for r in resistance_levels]
            
            # Prepare result
            result = {
                'analysis_days': self.analysis_days,
                'display_days': days,
                'analysis_date': datetime.now().isoformat(),
                'data_points': len(df),
                'current_price': latest_price,
                'latest_date': latest_date,
                'window_size': window,
                'detected_points': {
                    'support': len(min_idx),
                    'resistance': len(max_idx),
                    'total': len(min_idx) + len(max_idx)
                },
                'clustered_zones': len(all_zones),
                'support_levels': support_levels,
                'resistance_levels': resistance_levels,
                'nearest_support': support_levels[0] if support_levels else None,
                'nearest_resistance': resistance_levels[0] if resistance_levels else None,
                'all_zones': all_zones,
                'price_range': {
                    'min': minPrice,
                    'max': maxPrice,
                    'range': priceRange
                }
            }
            
            logger.info(f"S/R analysis completed (based on {self.analysis_days} days): "
                       f"{len(support_levels)} strong supports, {len(resistance_levels)} strong resistances")
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating support/resistance: {e}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}
    
    def get_detailed_analysis(self, days=175, window=None):
        """
        Get detailed support/resistance analysis with additional insights
        
        Parameters:
        - days: Display period (7, 30, 175, 365)
        - window: Detection sensitivity
        
        Returns:
        - Comprehensive analysis including price action context
        """
        analysis = self.calculate_support_resistance(days, window)
        
        if 'error' in analysis:
            return analysis
        
        # Add trading insights
        insights = []
        
        # Check if price is near support or resistance
        if analysis.get('nearest_support'):
            dist_pct = analysis['nearest_support']['distance_percent']
            if dist_pct < 1.0:
                insights.append(f"Price is very close to support level at {analysis['nearest_support']['level']:.2f}")
            elif dist_pct < 2.0:
                insights.append(f"Price is near support level at {analysis['nearest_support']['level']:.2f}")
        
        if analysis.get('nearest_resistance'):
            dist_pct = analysis['nearest_resistance']['distance_percent']
            if dist_pct < 1.0:
                insights.append(f"Price is very close to resistance level at {analysis['nearest_resistance']['level']:.2f}")
            elif dist_pct < 2.0:
                insights.append(f"Price is near resistance level at {analysis['nearest_resistance']['level']:.2f}")
        
        # Add insights to analysis
        analysis['insights'] = insights
        analysis['strength_indicators'] = {
            'support_strength': len(analysis['support_levels']),
            'resistance_strength': len(analysis['resistance_levels']),
            'overall_volatility': analysis['price_range']['range'] / analysis['current_price'] * 100
        }
        
        return analysis
    
    def get_line_chart_data(self, days=175):
        """
        Get simple line chart data (only index values with dates)
        Note: OHLC data not available, so candlestick charts cannot be created
        
        Parameters:
        - days: Number of days (7, 30, 175, 365)
        
        Returns:
        - List of price data points for line chart
        """
        try:
            history_data = self._get_data_by_days(days)
            
            if not history_data:
                return {'error': 'No historical data available'}
            
            # Prepare simple line chart data
            line_data = []
            
            for point in history_data:
                data_point = {
                    'date': point['date'],
                    'index_value': point['index_value'],
                    'percent_change': point.get('percent_change', 0),
                    'turnover': point.get('turnover', 0)
                }
                
                line_data.append(data_point)
            
            return {
                'days': days,
                'data': line_data,
                'count': len(line_data),
                'chart_type': 'line'
            }
            
        except Exception as e:
            logger.error(f"Error preparing line chart data: {e}")
            return {'error': str(e)}