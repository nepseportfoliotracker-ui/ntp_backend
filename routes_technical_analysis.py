# routes_technical_analysis.py - FIXED VERSION
# Fixes: RuntimeError: Working outside of application context

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
    
    @app.route('/api/analysis/chart-data', methods=['GET'])
    @require_auth
    def get_complete_chart_data():
        """
        Get complete data for charting (history + support/resistance)
        Query params: days (7/30/100/365), window (optional)
        Uses line chart data instead of candlestick (no OHLC available)
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
            
            # Get S/R analysis (always uses 100 days for S/R)
            analysis = technical_service.get_detailed_analysis(days, window)
            
            if 'error' in analysis:
                return jsonify({
                    'success': False,
                    'error': analysis['error'],
                    'flutter_ready': True
                }), 500
            
            # Get line chart data for selected period
            line_data_response = technical_service.get_line_chart_data(days)
            
            if 'error' in line_data_response:
                return jsonify({
                    'success': False,
                    'error': line_data_response['error'],
                    'flutter_ready': True
                }), 500
            
            # Format the data for Flutter
            candlestick_data = line_data_response.get('data', [])
            
            return jsonify({
                'success': True,
                'days': days,
                'analysis': analysis,
                'candlestick_data': candlestick_data,  # Actually line data, keeping name for compatibility
                'support_levels': analysis.get('support_levels', []),
                'resistance_levels': analysis.get('resistance_levels', []),
                'current_price': analysis.get('current_price'),
                'insights': analysis.get('insights', []),
                'note': 'Support/Resistance levels calculated from 100 days data. Chart shows line data (no OHLC available).',
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
            
        except Exception as e:
            logger.error(f"Error in complete chart data endpoint: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/analysis/support-resistance', methods=['GET'])
    @require_auth
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
    @require_auth
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
    
    @app.route('/api/analysis/summary', methods=['GET'])
    @require_auth
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
    
    logger.info("✅ Technical analysis routes registered successfully with line chart data support")