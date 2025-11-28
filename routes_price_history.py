# routes_price_history.py - API routes for persistent price history

import logging
from flask import Blueprint, jsonify, request
from datetime import datetime

logger = logging.getLogger(__name__)


def register_price_history_routes(app):
    """Register all price history routes"""
    
    @app.route('/api/price-history/symbol/<symbol>', methods=['GET'])
    def get_symbol_price_history(symbol):
        """
        Get price history for a specific symbol (up to 30 days).
        
        Query params:
            - days: Number of days to retrieve (default 30)
        """
        try:
            price_history_service = app.config.get('price_history_service')
            if not price_history_service:
                return jsonify({'error': 'Price history service not available'}), 500
            
            days = request.args.get('days', 30, type=int)
            if days < 1 or days > 30:
                days = 30
            
            history = price_history_service.get_price_history(symbol, days=days)
            
            if not history:
                return jsonify({
                    'symbol': symbol.upper(),
                    'message': 'No price history available',
                    'data': []
                }), 200
            
            return jsonify({
                'symbol': symbol.upper(),
                'records': len(history),
                'first_date': history[0]['date'],
                'last_date': history[-1]['date'],
                'data': history
            }), 200
        
        except Exception as e:
            logger.error(f"Error retrieving price history for {symbol}: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/price-history/stats/<symbol>', methods=['GET'])
    def get_symbol_history_stats(symbol):
        """Get statistics about price history for a symbol"""
        try:
            price_history_service = app.config.get('price_history_service')
            if not price_history_service:
                return jsonify({'error': 'Price history service not available'}), 500
            
            stats = price_history_service.get_price_history_stats(symbol)
            return jsonify(stats), 200
        
        except Exception as e:
            logger.error(f"Error getting history stats for {symbol}: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/price-history/all-stats', methods=['GET'])
    def get_all_history_stats():
        """Get statistics for all symbols in price history"""
        try:
            price_history_service = app.config.get('price_history_service')
            if not price_history_service:
                return jsonify({'error': 'Price history service not available'}), 500
            
            stats = price_history_service.get_all_symbols_stats()
            
            return jsonify({
                'total_symbols': len(stats),
                'symbols': stats
            }), 200
        
        except Exception as e:
            logger.error(f"Error getting all history stats: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/price-history/database-info', methods=['GET'])
    def get_history_database_info():
        """Get information about the price history database (persistent)"""
        try:
            price_history_service = app.config.get('price_history_service')
            if not price_history_service:
                return jsonify({'error': 'Price history service not available'}), 500
            
            info = price_history_service.get_history_database_info()
            return jsonify(info), 200
        
        except Exception as e:
            logger.error(f"Error getting history database info: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/price-history/cleanup', methods=['POST'])
    def cleanup_history():
        """
        Clean up invalid or duplicate records in price history.
        Requires admin authentication.
        """
        try:
            require_admin = app.config.get('require_admin')
            if require_admin:
                require_admin()
            
            price_history_service = app.config.get('price_history_service')
            if not price_history_service:
                return jsonify({'error': 'Price history service not available'}), 500
            
            result = price_history_service.cleanup_invalid_records()
            
            return jsonify({
                'success': True,
                'message': 'Price history cleanup completed',
                'details': result
            }), 200
        
        except Exception as e:
            logger.error(f"Error during history cleanup: {e}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/price-history/save-daily', methods=['POST'])
    def save_daily_prices():
        """
        Save today's closing prices to persistent history.
        Requires admin authentication.
        Should be called after market close at 3:00 PM.
        """
        try:
            require_admin = app.config.get('require_admin')
            if require_admin:
                require_admin()
            
            price_history_service = app.config.get('price_history_service')
            price_service = app.config.get('price_service')
            
            if not price_history_service or not price_service:
                return jsonify({'error': 'Required services not available'}), 500
            
            # Get all current stocks
            stocks_data = price_service.get_all_stocks()
            
            if not stocks_data:
                return jsonify({
                    'success': False,
                    'error': 'No stock data available to save'
                }), 400
            
            # Save to persistent history
            result = price_history_service.save_daily_prices(stocks_data)
            
            return jsonify(result), 200 if result['success'] else 400
        
        except Exception as e:
            logger.error(f"Error saving daily prices: {e}")
            return jsonify({'error': str(e)}), 500