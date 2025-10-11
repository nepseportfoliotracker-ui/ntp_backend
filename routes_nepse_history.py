# routes_nepse_history.py - NEPSE History API Routes

import logging
from datetime import datetime
from flask import jsonify, request

logger = logging.getLogger(__name__)


def register_nepse_history_routes(app):
    """Register NEPSE history routes for Flutter app"""
    
    nepse_history_service = app.config.get('nepse_history_service')
    require_auth = app.config.get('require_auth')
    require_admin = app.config.get('require_admin')
    
    # ==================== PUBLIC ENDPOINTS ====================
    
    @app.route('/api/nepse/history/weekly', methods=['GET'])
    def get_weekly_history():
        """Get NEPSE index history for the last 7 days"""
        try:
            data = nepse_history_service.get_weekly_data()
            stats = nepse_history_service.get_statistics('weekly')
            
            return jsonify({
                'success': True,
                'period': 'weekly',
                'data': data,
                'statistics': stats,
                'count': len(data),
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            logger.error(f"Error fetching weekly history: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/nepse/history/monthly', methods=['GET'])
    def get_monthly_history():
        """Get NEPSE index history for the last 30 days"""
        try:
            data = nepse_history_service.get_monthly_data()
            stats = nepse_history_service.get_statistics('monthly')
            
            return jsonify({
                'success': True,
                'period': 'monthly',
                'data': data,
                'statistics': stats,
                'count': len(data),
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            logger.error(f"Error fetching monthly history: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/nepse/history/yearly', methods=['GET'])
    def get_yearly_history():
        """Get NEPSE index history for the last 365 days"""
        try:
            data = nepse_history_service.get_yearly_data()
            stats = nepse_history_service.get_statistics('yearly')
            
            return jsonify({
                'success': True,
                'period': 'yearly',
                'data': data,
                'statistics': stats,
                'count': len(data),
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            logger.error(f"Error fetching yearly history: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/nepse/history/all', methods=['GET'])
    def get_all_history():
        """Get all NEPSE index history data (weekly, monthly, yearly)"""
        try:
            metadata = nepse_history_service.get_metadata()
            
            return jsonify({
                'success': True,
                'data': {
                    'weekly': nepse_history_service.get_weekly_data(),
                    'monthly': nepse_history_service.get_monthly_data(),
                    'yearly': nepse_history_service.get_yearly_data()
                },
                'statistics': {
                    'weekly': nepse_history_service.get_statistics('weekly'),
                    'monthly': nepse_history_service.get_statistics('monthly'),
                    'yearly': nepse_history_service.get_statistics('yearly')
                },
                'metadata': metadata,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            logger.error(f"Error fetching all history: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/nepse/history/statistics', methods=['GET'])
    def get_history_statistics():
        """Get statistics for all periods"""
        try:
            period = request.args.get('period', 'monthly')
            
            if period not in ['weekly', 'monthly', 'yearly', 'all']:
                return jsonify({
                    'success': False,
                    'error': 'Invalid period. Use: weekly, monthly, yearly, or all',
                    'flutter_ready': True
                }), 400
            
            if period == 'all':
                stats = {
                    'weekly': nepse_history_service.get_statistics('weekly'),
                    'monthly': nepse_history_service.get_statistics('monthly'),
                    'yearly': nepse_history_service.get_statistics('yearly')
                }
            else:
                stats = nepse_history_service.get_statistics(period)
            
            return jsonify({
                'success': True,
                'statistics': stats,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            logger.error(f"Error fetching statistics: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/nepse/history/metadata', methods=['GET'])
    def get_history_metadata():
        """Get metadata about all history tables"""
        try:
            metadata = nepse_history_service.get_metadata()
            
            return jsonify({
                'success': True,
                'metadata': metadata,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            logger.error(f"Error fetching metadata: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    # ==================== ADMIN ENDPOINTS ====================
    
    @app.route('/api/admin/nepse/history/scrape', methods=['POST'])
    @require_admin
    def admin_scrape_history():
        """Admin endpoint to manually trigger history scraping"""
        try:
            data = request.get_json() or {}
            period = data.get('period', 'all')
            force = data.get('force', False)
            
            if period == 'all':
                results = nepse_history_service.scrape_all_periods(force)
            elif period == 'weekly':
                results = {'weekly': nepse_history_service.scrape_weekly_data(force)}
            elif period == 'monthly':
                results = {'monthly': nepse_history_service.scrape_monthly_data(force)}
            elif period == 'yearly':
                results = {'yearly': nepse_history_service.scrape_yearly_data(force)}
            else:
                return jsonify({
                    'success': False,
                    'error': 'Invalid period. Use: weekly, monthly, yearly, or all',
                    'flutter_ready': True
                }), 400
            
            return jsonify({
                'success': True,
                'message': 'History scraping completed',
                'results': results,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            logger.error(f"Error in admin scrape: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/admin/nepse/history/clean', methods=['POST'])
    @require_admin
    def admin_clean_history():
        """Admin endpoint to clean old historical data"""
        try:
            nepse_history_service.clean_old_data()
            
            return jsonify({
                'success': True,
                'message': 'Old historical data cleaned successfully',
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            logger.error(f"Error cleaning history: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    logger.info("NEPSE history routes registered successfully")