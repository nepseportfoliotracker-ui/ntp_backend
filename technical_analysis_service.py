# technical_analysis_service.py - Updated with specific day counts

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
        self.merge_threshold = 0.015  # 1.5% threshold for merging nearby levels
        self.max_clusters = 5  # Maximum number of support/resistance zones
        self.analysis_days = 100  # FIXED: Always use 100 days for S/R analysis
        self.strength_threshold = 0.75  # Show levels with 75%+ strength
    
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
        - days: Number of days (7, 30, 100, 365)
        
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
        
        Parameters:
        - prices: Array of price values
        - window: Sensitivity parameter (larger = smoother)
        
        Returns:
        - min_indices: Indices of local minima
        - max_indices: Indices of local maxima
        """
        min_idx = argrelextrema(prices, np.less_equal, order=window)[0]
        max_idx = argrelextrema(prices, np.greater_equal, order=window)[0]
        
        return min_idx, max_idx
    
    def _cluster_levels(self, levels, num_clusters=None):
        """
        Cluster detected support/resistance levels using KMeans
        
        Parameters:
        - levels: List of price levels
        - num_clusters: Number of clusters (auto if None)
        
        Returns:
        - Sorted array of cluster centers
        """
        if len(levels) == 0:
            return np.array([])
        
        levels_array = np.array(levels).reshape(-1, 1)
        
        if num_clusters is None:
            num_clusters = min(self.max_clusters, len(levels))
        
        if num_clusters < 1:
            return np.array([])
        
        try:
            kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init='auto')
            kmeans.fit(levels_array)
            zones = sorted(kmeans.cluster_centers_.flatten())
            return zones
        except Exception as e:
            logger.error(f"KMeans clustering failed: {e}")
            return np.array([])
    
    def _filter_nearby_zones(self, zones, threshold=0.005):
        """
        Filter out zones that are too close to each other
        
        Parameters:
        - zones: Array of zone values
        - threshold: Minimum percentage difference between zones
        
        Returns:
        - Filtered list of zones
        """
        if len(zones) == 0:
            return []
        
        filtered = []
        for z in zones:
            if not filtered:
                filtered.append(z)
            elif abs(z - filtered[-1]) / filtered[-1] > threshold:
                filtered.append(z)
        
        return filtered
    
    def calculate_support_resistance(self, days=100, window=None):
        """
        Calculate support and resistance levels for given number of days
        IMPORTANT: S/R analysis always uses 100 days data regardless of display period
        
        Parameters:
        - days: Number of days for display (7, 30, 100, 365)
        - window: Sensitivity for extrema detection (default: 5)
        
        Returns:
        - Dictionary with support/resistance analysis
        """
        try:
            # ALWAYS use 100 days for S/R calculation
            analysis_data = self._get_data_by_days(self.analysis_days)
            
            if not analysis_data:
                return {'error': 'No historical data available'}
            
            # Prepare DataFrame
            df = self._prepare_dataframe(analysis_data)
            if df.empty:
                return {'error': 'Failed to prepare data'}
            
            # Use custom window or default
            window = window or self.default_window
            
            # Detect local extrema
            prices = df['index_value'].values
            min_idx, max_idx = self._detect_local_extrema(prices, window)
            
            logger.info(f"Detected {len(min_idx)} support points and {len(max_idx)} resistance points from {self.analysis_days} days")
            
            # Extract support and resistance values
            support_values = df.iloc[min_idx]['index_value'].tolist()
            resistance_values = df.iloc[max_idx]['index_value'].tolist()
            
            # Cluster the levels
            all_levels = support_values + resistance_values
            clustered_zones = self._cluster_levels(all_levels)
            
            # Filter nearby zones
            filtered_zones = self._filter_nearby_zones(clustered_zones, self.merge_threshold)
            
            # Get latest price
            latest_price = df['index_value'].iloc[-1]
            latest_date = df.index[-1].strftime('%Y-%m-%d')
            
            # Classify zones as support or resistance
            supports = [float(z) for z in filtered_zones if z < latest_price]
            resistances = [float(z) for z in filtered_zones if z > latest_price]
            
            # Calculate strength based on touches
            support_levels = []
            for s in supports:
                # Count how many times price came close to this level
                touches = sum(1 for price in prices if abs(price - s) / s < 0.01)
                strength = min(1.0, 0.7 + (touches * 0.05))
                
                support_levels.append({
                    'level': s,
                    'strength': strength,
                    'touches': touches,
                    'distance': latest_price - s,
                    'distance_percent': ((latest_price - s) / latest_price) * 100
                })
            
            resistance_levels = []
            for r in resistances:
                touches = sum(1 for price in prices if abs(price - r) / r < 0.01)
                strength = min(1.0, 0.7 + (touches * 0.05))
                
                resistance_levels.append({
                    'level': r,
                    'strength': strength,
                    'touches': touches,
                    'distance': r - latest_price,
                    'distance_percent': ((r - latest_price) / latest_price) * 100
                })
            
            # Sort by strength descending
            support_levels.sort(key=lambda x: x['strength'], reverse=True)
            resistance_levels.sort(key=lambda x: x['strength'], reverse=True)
            
            # Filter to only show strong levels (strength >= 0.85)
            support_levels = [s for s in support_levels if s['strength'] >= 0.85]
            resistance_levels = [r for r in resistance_levels if r['strength'] >= 0.85]
            
            # Prepare result
            result = {
                'analysis_days': self.analysis_days,
                'display_days': days,
                'analysis_date': datetime.now().isoformat(),
                'data_points': len(df),
                'current_price': float(latest_price),
                'latest_date': latest_date,
                'window_size': window,
                'detected_points': {
                    'support': len(min_idx),
                    'resistance': len(max_idx),
                    'total': len(min_idx) + len(max_idx)
                },
                'clustered_zones': len(filtered_zones),
                'support_levels': support_levels,
                'resistance_levels': resistance_levels,
                'nearest_support': support_levels[0] if support_levels else None,
                'nearest_resistance': resistance_levels[0] if resistance_levels else None,
                'all_zones': [float(z) for z in filtered_zones],
                'price_range': {
                    'min': float(df['index_value'].min()),
                    'max': float(df['index_value'].max()),
                    'range': float(df['index_value'].max() - df['index_value'].min())
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
    
    def get_detailed_analysis(self, days=100, window=None):
        """
        Get detailed support/resistance analysis with additional insights
        
        Parameters:
        - days: Display period (7, 30, 100, 365)
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
    
    def get_line_chart_data(self, days=100):
        """
        Get simple line chart data (only index values with dates)
        Note: OHLC data not available, so candlestick charts cannot be created
        
        Parameters:
        - days: Number of days (7, 30, 100, 365)
        
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