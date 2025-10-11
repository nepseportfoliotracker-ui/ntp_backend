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
        self.default_window = 5
        self.merge_threshold = 0.005
        self.max_clusters = 5
        self.strength_threshold = 0.85  # Only show levels with 85%+ strength
    
    def _prepare_dataframe(self, history_data):
        """Convert history data to pandas DataFrame"""
        if not history_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(history_data)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date', ascending=True)
        df = df.dropna(subset=['index_value'])
        df.set_index('date', inplace=True)
        
        return df
    
    def _detect_local_extrema(self, prices, window=5):
        """Detect local minima (support) and maxima (resistance) points"""
        min_idx = argrelextrema(prices, np.less_equal, order=window)[0]
        max_idx = argrelextrema(prices, np.greater_equal, order=window)[0]
        
        return min_idx, max_idx
    
    def _cluster_levels(self, levels, num_clusters=None):
        """Cluster detected support/resistance levels using KMeans"""
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
        """Filter out zones that are too close to each other"""
        if len(zones) == 0:
            return []
        
        filtered = []
        for z in zones:
            if not filtered:
                filtered.append(z)
            elif abs(z - filtered[-1]) / filtered[-1] > threshold:
                filtered.append(z)
        
        return filtered
    
    def _calculate_zone_strength(self, prices, zone, tolerance=0.02):
        """Calculate strength of a support/resistance zone based on touches"""
        price_range = prices.max() - prices.min()
        zone_tolerance = price_range * tolerance
        
        touches = np.sum(np.abs(prices - zone) <= zone_tolerance)
        max_touches = len(prices) * 0.1
        
        strength = min(1.0, touches / max_touches) if max_touches > 0 else 0
        return strength, int(touches)
    
    def calculate_support_resistance(self, period='monthly', window=None):
        """Calculate support and resistance levels for given period"""
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
            
            df = self._prepare_dataframe(history_data)
            if df.empty:
                return {'error': 'Failed to prepare data'}
            
            window = window or self.default_window
            prices = df['index_value'].values
            min_idx, max_idx = self._detect_local_extrema(prices, window)
            
            logger.info(f"Detected {len(min_idx)} support points and {len(max_idx)} resistance points")
            
            support_values = df.iloc[min_idx]['index_value'].tolist()
            resistance_values = df.iloc[max_idx]['index_value'].tolist()
            
            all_levels = support_values + resistance_values
            clustered_zones = self._cluster_levels(all_levels)
            filtered_zones = self._filter_nearby_zones(clustered_zones, self.merge_threshold)
            
            latest_price = df['index_value'].iloc[-1]
            latest_date = df.index[-1].strftime('%Y-%m-%d')
            
            # Calculate strength for each zone and filter by strength threshold
            strong_zones = []
            for zone in filtered_zones:
                strength, touches = self._calculate_zone_strength(prices, zone)
                if strength >= self.strength_threshold:
                    strong_zones.append({'zone': zone, 'strength': strength, 'touches': touches})
            
            # Sort by strength
            strong_zones.sort(key=lambda x: x['strength'], reverse=True)
            
            supports = [float(s['zone']) for s in strong_zones if s['zone'] < latest_price]
            resistances = [float(r['zone']) for r in strong_zones if r['zone'] > latest_price]
            
            support_distances = [
                {
                    'level': s,
                    'distance': latest_price - s,
                    'distance_percent': ((latest_price - s) / latest_price) * 100,
                    'strength': next(x['strength'] for x in strong_zones if x['zone'] == s)
                }
                for s in supports
            ]
            
            resistance_distances = [
                {
                    'level': r,
                    'distance': r - latest_price,
                    'distance_percent': ((r - latest_price) / latest_price) * 100,
                    'strength': next(x['strength'] for x in strong_zones if x['zone'] == r)
                }
                for r in resistances
            ]
            
            support_distances.sort(key=lambda x: x['distance'])
            resistance_distances.sort(key=lambda x: x['distance'])
            
            result = {
                'period': period,
                'analysis_date': datetime.now().isoformat(),
                'data_points': len(df),
                'current_price': float(latest_price),
                'latest_date': latest_date,
                'window_size': window,
                'strength_threshold': self.strength_threshold,
                'detected_points': {
                    'support': len(min_idx),
                    'resistance': len(max_idx),
                    'total': len(min_idx) + len(max_idx)
                },
                'strong_zones_count': len(strong_zones),
                'support_levels': support_distances,
                'resistance_levels': resistance_distances,
                'nearest_support': support_distances[0] if support_distances else None,
                'nearest_resistance': resistance_distances[0] if resistance_distances else None,
                'all_zones': [float(s['zone']) for s in strong_zones],
                'price_range': {
                    'min': float(df['index_value'].min()),
                    'max': float(df['index_value'].max()),
                    'range': float(df['index_value'].max() - df['index_value'].min())
                }
            }
            
            logger.info(f"Support/Resistance analysis completed for {period}: "
                       f"{len(supports)} strong supports, {len(resistances)} strong resistances")
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating support/resistance: {e}")
            import traceback
            traceback.print_exc()
            return {'error': str(e)}
    
    def get_detailed_analysis(self, period='monthly', window=None):
        """Get detailed support/resistance analysis with additional insights"""
        analysis = self.calculate_support_resistance(period, window)
        
        if 'error' in analysis:
            return analysis
        
        insights = []
        
        if analysis.get('nearest_support'):
            dist_pct = analysis['nearest_support']['distance_percent']
            strength = analysis['nearest_support']['strength']
            if dist_pct < 1.0:
                insights.append({
                    'type': 'warning',
                    'message': f"Price is very close to strong support at {analysis['nearest_support']['level']:.2f}",
                    'level': analysis['nearest_support']['level'],
                    'distance_percent': dist_pct,
                    'strength': strength
                })
        
        if analysis.get('nearest_resistance'):
            dist_pct = analysis['nearest_resistance']['distance_percent']
            strength = analysis['nearest_resistance']['strength']
            if dist_pct < 1.0:
                insights.append({
                    'type': 'warning',
                    'message': f"Price is very close to strong resistance at {analysis['nearest_resistance']['level']:.2f}",
                    'level': analysis['nearest_resistance']['level'],
                    'distance_percent': dist_pct,
                    'strength': strength
                })
        
        analysis['insights'] = insights
        analysis['strength_indicators'] = {
            'support_strength': len(analysis['support_levels']),
            'resistance_strength': len(analysis['resistance_levels']),
            'overall_volatility': analysis['price_range']['range'] / analysis['current_price'] * 100
        }
        
        return analysis