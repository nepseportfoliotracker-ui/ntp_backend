# routes_technical_analysis.py - Updated with day-based parameters

import logging
from datetime import datetime
from flask import jsonify, request

logger = logging.getLogger(__name__)

# Valid day options
VALID_DAYS = [7, 30, 100, 365]


def register_technical_analysis_routes(app):
    """Register technical analysis routes for Flutter app"""
    
    technical_service = app.config.get('technical_analysis_service')
    require_auth = app.config.get('require_auth')
    
    # ==================== PUBLIC ENDPOINTS ====================
    
    @app.route('/api/analysis/support-resistance', methods=['GET'])
    def get_support_resistance():
        """
        Get support and resistance levels
        Query params: days (7/30/100/365), window (optional)
        Note: S/R analysis always uses 100 days data
        """
        try:
            days = request.args.get('days', default=100, type=int)
            window = request.args.get('window', type=int)
            
            if days not in VALID_DAYS:
                return jsonify({
                    'success': False,
                    'error': f'Invalid days. Use: {", ".join(map(str, VALID_DAYS))}',
                    'flutter_ready': True
                }), 400
            
            analysis = technical_service.calculate_support_resistance(days, window)
            
            if 'error' in analysis:
                return jsonify({
                    'success': False,
                    'error': analysis['error'],
                    'flutter_ready': True
                }), 500
            
            return jsonify({
                'success': True,
                'analysis': analysis,
                'note': 'Support/Resistance calculated using 100 days data',
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
            
        except Exception as e:
            logger.error(f"Error in support-resistance endpoint: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/analysis/detailed', methods=['GET'])
    def get_detailed_analysis():
        """
        Get detailed technical analysis with insights
        Query params: days (7/30/100/365), window (optional)
        """
        try:
            days = request.args.get('days', default=100, type=int)
            window = request.args.get('window', type=int)
            
            if days not in VALID_DAYS:
                return jsonify({
                    'success': False,
                    'error': f'Invalid days. Use: {", ".join(map(str, VALID_DAYS))}',
                    'flutter_ready': True
                }), 400
            
            analysis = technical_service.get_detailed_analysis(days, window)
            
            if 'error' in analysis:
                return jsonify({
                    'success': False,
                    'error': analysis['error'],
                    'flutter_ready': True
                }), 500
            
            return jsonify({
                'success': True,
                'analysis': analysis,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
            
        except Exception as e:
            logger.error(f"Error in detailed analysis endpoint: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/analysis/candlestick', methods=['GET'])
    def get_candlestick_data():
        """
        Get candlestick data for charts
        Query params: days (7/30/100/365)
        """
        try:
            days = request.args.get('days', default=100, type=int)
            
            if days not in VALID_DAYS:
                return jsonify({
                    'success': False,
                    'error': f'Invalid days. Use: {", ".join(map(str, VALID_DAYS))}',
                    'flutter_ready': True
                }), 400
            
            candlestick_data = technical_service.get_candlestick_data(days)
            
            if 'error' in candlestick_data:
                return jsonify({
                    'success': False,
                    'error': candlestick_data['error'],
                    'flutter_ready': True
                }), 500
            
            return jsonify({
                'success': True,
                'candlestick_data': candlestick_data,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
            
        except Exception as e:
            logger.error(f"Error in candlestick endpoint: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/analysis/chart-data', methods=['GET'])
    def get_complete_chart_data():
        """
        Get complete data for charting (history + support/resistance)
        Query params: days (7/30/100/365), window (optional)
        """
        try:
            days = request.args.get('days', default=100, type=int)
            window = request.args.get('window', type=int)
            
            if days not in VALID_DAYS:
                return jsonify({
                    'success': False,
                    'error': f'Invalid days. Use: {", ".join(map(str, VALID_DAYS))}',
                    'flutter_ready': True
                }), 400
            
            # Get analysis (always uses 100 days for S/R)
            analysis = technical_service.get_detailed_analysis(days, window)
            
            if 'error' in analysis:
                return jsonify({
                    'success': False,
                    'error': analysis['error'],
                    'flutter_ready': True
                }), 500
            
            # Get candlestick data for selected period
            candlestick_data = technical_service.get_candlestick_data(days)
            
            return jsonify({
                'success': True,
                'days': days,
                'analysis': analysis,
                'candlestick_data': candlestick_data.get('data', []),
                'support_levels': analysis.get('support_levels', []),
                'resistance_levels': analysis.get('resistance_levels', []),
                'current_price': analysis.get('current_price'),
                'insights': analysis.get('insights', []),
                'note': 'Support/Resistance levels calculated from 100 days data',
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
            
        except Exception as e:
            logger.error(f"Error in complete chart data endpoint: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/analysis/summary', methods=['GET'])
    def get_analysis_summary():
        """
        Get quick summary of all day periods
        """
        try:
            summary = {}
            
            for days in VALID_DAYS:
                analysis = technical_service.calculate_support_resistance(days)
                
                if 'error' not in analysis:
                    summary[f'{days}days'] = {
                        'days': days,
                        'current_price': analysis.get('current_price'),
                        'support_count': len(analysis.get('support_levels', [])),
                        'resistance_count': len(analysis.get('resistance_levels', [])),
                        'nearest_support': analysis.get('nearest_support'),
                        'nearest_resistance': analysis.get('nearest_resistance'),
                        'price_range': analysis.get('price_range'),
                        'data_points': analysis.get('data_points')
                    }
            
            return jsonify({
                'success': True,
                'summary': summary,
                'note': 'All S/R levels calculated from 100 days data',
                'available_periods': VALID_DAYS,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
            
        except Exception as e:
            logger.error(f"Error in summary endpoint: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    logger.info("Technical analysis routes registered successfully with day-based parameters")