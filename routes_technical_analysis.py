# routes_technical_analysis.py - Technical Analysis API Routes

import logging
from datetime import datetime
from flask import jsonify, request

logger = logging.getLogger(__name__)


def register_technical_analysis_routes(app):
    """Register technical analysis routes for Flutter app"""
    
    technical_service = app.config.get('technical_analysis_service')
    require_auth = app.config.get('require_auth')
    
    # ==================== PUBLIC ENDPOINTS ====================
    
    @app.route('/api/analysis/support-resistance', methods=['GET'])
    def get_support_resistance():
        """
        Get support and resistance levels
        Query params: period (weekly/monthly/yearly), window (optional)
        """
        try:
            period = request.args.get('period', 'monthly')
            window = request.args.get('window', type=int)
            
            if period not in ['weekly', 'monthly', 'yearly']:
                return jsonify({
                    'success': False,
                    'error': 'Invalid period. Use: weekly, monthly, or yearly',
                    'flutter_ready': True
                }), 400
            
            analysis = technical_service.calculate_support_resistance(period, window)
            
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
        Query params: period (weekly/monthly/yearly), window (optional)
        """
        try:
            period = request.args.get('period', 'monthly')
            window = request.args.get('window', type=int)
            
            if period not in ['weekly', 'monthly', 'yearly']:
                return jsonify({
                    'success': False,
                    'error': 'Invalid period. Use: weekly, monthly, or yearly',
                    'flutter_ready': True
                }), 400
            
            analysis = technical_service.get_detailed_analysis(period, window)
            
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
        Query params: period (weekly/monthly/yearly)
        """
        try:
            period = request.args.get('period', 'monthly')
            
            if period not in ['weekly', 'monthly', 'yearly']:
                return jsonify({
                    'success': False,
                    'error': 'Invalid period. Use: weekly, monthly, or yearly',
                    'flutter_ready': True
                }), 400
            
            candlestick_data = technical_service.get_candlestick_data(period)
            
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
        Query params: period (weekly/monthly/yearly), window (optional)
        """
        try:
            period = request.args.get('period', 'monthly')
            window = request.args.get('window', type=int)
            
            if period not in ['weekly', 'monthly', 'yearly']:
                return jsonify({
                    'success': False,
                    'error': 'Invalid period. Use: weekly, monthly, or yearly',
                    'flutter_ready': True
                }), 400
            
            # Get analysis
            analysis = technical_service.get_detailed_analysis(period, window)
            
            if 'error' in analysis:
                return jsonify({
                    'success': False,
                    'error': analysis['error'],
                    'flutter_ready': True
                }), 500
            
            # Get candlestick data
            candlestick_data = technical_service.get_candlestick_data(period)
            
            return jsonify({
                'success': True,
                'period': period,
                'analysis': analysis,
                'candlestick_data': candlestick_data.get('data', []),
                'support_levels': analysis.get('support_levels', []),
                'resistance_levels': analysis.get('resistance_levels', []),
                'current_price': analysis.get('current_price'),
                'insights': analysis.get('insights', []),
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
        Get quick summary of all periods
        """
        try:
            summary = {}
            
            for period in ['weekly', 'monthly', 'yearly']:
                analysis = technical_service.calculate_support_resistance(period)
                
                if 'error' not in analysis:
                    summary[period] = {
                        'current_price': analysis.get('current_price'),
                        'support_count': len(analysis.get('support_levels', [])),
                        'resistance_count': len(analysis.get('resistance_levels', [])),
                        'nearest_support': analysis.get('nearest_support'),
                        'nearest_resistance': analysis.get('nearest_resistance'),
                        'price_range': analysis.get('price_range')
                    }
            
            return jsonify({
                'success': True,
                'summary': summary,
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
    
    logger.info("Technical analysis routes registered successfully")