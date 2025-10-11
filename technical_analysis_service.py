# technical_analysis_service.py - Support & Resistance Analysis Service

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
    
    def calculate_support_resistance(self, period='monthly', window=None):
        """
        Calculate support and resistance levels for given period
        
        Parameters:
        - period: 'weekly', 'monthly', or 'yearly'
        - window: Sensitivity for extrema detection (default: 5)
        
        Returns:
        - Dictionary with support/resistance analysis
        """
        try:
            # Get historical data
            if period == 'weekly':
                history_data = self.nepse_history_service.get_weekly_data()
            elif period == 'monthly':
                history_data = self.nepse_history_service.get_monthly_data()
            elif period == 'yearly':
                history_data = self.nepse_history_service.get_yearly_data()
            else:
                return {'error': f'Invalid period: {period}'}
            
            if not history_data:
                return {'error': 'No historical data available'}
            
            # Prepare DataFrame
            df = self._prepare_dataframe(history_data)
            if df.empty:
                return {'error': 'Failed to prepare data'}
            
            # Use custom window or default
            window = window or self.default_window
            
            # Detect local extrema
            prices = df['index_value'].values
            min_idx, max_idx = self._detect_local_extrema(prices, window)
            
            logger.info(f"Detected {len(min_idx)} support points and {len(max_idx)} resistance points")
            
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
            
            # Calculate distances from current price
            support_distances = [
                {
                    'level': s,
                    'distance': latest_price - s,
                    'distance_percent': ((latest_price - s) / latest_price) * 100
                }
                for s in supports
            ]
            
            resistance_distances = [
                {
                    'level': r,
                    'distance': r - latest_price,
                    'distance_percent': ((r - latest_price) / latest_price) * 100
                }
                for r in resistances
            ]
            
            # Sort by distance (closest first)
            support_distances.sort(key=lambda x: x['distance'])
            resistance_distances.sort(key=lambda x: x['distance'])
            
            # Prepare result
            result = {
                'period': period,
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
                'support_levels': support_distances,
                'resistance_levels': resistance_distances,
                'nearest_support': support_distances[0] if support_distances else None,
                'nearest_resistance': resistance_distances[0] if resistance_distances else None,
                'all_zones': [float(z) for z in filtered_zones],
                'price_range': {
                    'min': float(df['index_value'].min()),
                    'max': float(df['index_value'].max()),
                    'range': float(df['index_value'].max() - df['index_value'].min())
                }
            }
            
            logger.info(f"Support/Resistance analysis completed for {period}: "
                       f"{len(supports)} supports, {len(resistances)} resistances")
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating support/resistance: {e}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}
    
    def get_detailed_analysis(self, period='monthly', window=None):
        """
        Get detailed support/resistance analysis with additional insights
        
        Returns:
        - Comprehensive analysis including price action context
        """
        analysis = self.calculate_support_resistance(period, window)
        
        if 'error' in analysis:
            return analysis
        
        # Add trading insights
        insights = []
        
        # Check if price is near support or resistance
        if analysis.get('nearest_support'):
            dist_pct = analysis['nearest_support']['distance_percent']
            if dist_pct < 1.0:
                insights.append({
                    'type': 'warning',
                    'message': f"Price is very close to support level at {analysis['nearest_support']['level']:.2f}",
                    'level': analysis['nearest_support']['level'],
                    'distance_percent': dist_pct
                })
            elif dist_pct < 2.0:
                insights.append({
                    'type': 'info',
                    'message': f"Price is near support level at {analysis['nearest_support']['level']:.2f}",
                    'level': analysis['nearest_support']['level'],
                    'distance_percent': dist_pct
                })
        
        if analysis.get('nearest_resistance'):
            dist_pct = analysis['nearest_resistance']['distance_percent']
            if dist_pct < 1.0:
                insights.append({
                    'type': 'warning',
                    'message': f"Price is very close to resistance level at {analysis['nearest_resistance']['level']:.2f}",
                    'level': analysis['nearest_resistance']['level'],
                    'distance_percent': dist_pct
                })
            elif dist_pct < 2.0:
                insights.append({
                    'type': 'info',
                    'message': f"Price is near resistance level at {analysis['nearest_resistance']['level']:.2f}",
                    'level': analysis['nearest_resistance']['level'],
                    'distance_percent': dist_pct
                })
        
        # Add strength indicators
        analysis['insights'] = insights
        analysis['strength_indicators'] = {
            'support_strength': len(analysis['support_levels']),
            'resistance_strength': len(analysis['resistance_levels']),
            'overall_volatility': analysis['price_range']['range'] / analysis['current_price'] * 100
        }
        
        return analysis
    
    def get_candlestick_data(self, period='monthly'):
        """
        Get OHLC-like data for candlestick charts
        (Using NEPSE index as close, calculating approximate OHLC from index movements)
        
        Returns:
        - List of candlestick data points
        """
        try:
            if period == 'weekly':
                history_data = self.nepse_history_service.get_weekly_data()
            elif period == 'monthly':
                history_data = self.nepse_history_service.get_monthly_data()
            elif period == 'yearly':
                history_data = self.nepse_history_service.get_yearly_data()
            else:
                return {'error': f'Invalid period: {period}'}
            
            if not history_data:
                return {'error': 'No historical data available'}
            
            # Prepare candlestick-like data
            candlestick_data = []
            
            for i, point in enumerate(history_data):
                # For simplicity, use index_value as close
                # Calculate approximate high/low based on volatility
                close = point['index_value']
                
                # Estimate high/low from percent change
                pct_change = abs(point.get('percent_change', 0))
                estimated_range = close * (pct_change / 100) if pct_change else close * 0.01
                
                # Create OHLC structure
                if i > 0:
                    prev_close = history_data[i - 1]['index_value']
                else:
                    prev_close = close
                
                candle = {
                    'date': point['date'],
                    'open': prev_close,
                    'high': max(close, prev_close) + estimated_range / 2,
                    'low': min(close, prev_close) - estimated_range / 2,
                    'close': close,
                    'volume': point.get('turnover', 0),
                    'color': 'green' if close >= prev_close else 'red'
                }
                
                candlestick_data.append(candle)
            
            return {
                'period': period,
                'data': candlestick_data,
                'count': len(candlestick_data)
            }
            
        except Exception as e:
            logger.error(f"Error preparing candlestick data: {e}")
            return {'error': str(e)}