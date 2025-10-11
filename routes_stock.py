# routes_stock.py - Stock Routes

import logging
from datetime import datetime
from flask import jsonify, request

logger = logging.getLogger(__name__)


def register_stock_routes(app):
    """Register stock-related routes"""
    
    # Get decorators from app config
    require_auth = app.config['require_auth']
    
    @app.route('/api/stocks', methods=['GET'])
    @require_auth
    def get_stocks():
        """Get all stock data"""
        try:
            services = {
                'price_service': app.config['price_service'],
                'scraping_service': app.config['scraping_service']
            }
            
            symbol = request.args.get('symbol')
            
            if symbol:
                data = services['price_service'].get_stock_by_symbol(symbol)
                if not data:
                    return jsonify({
                        'success': False,
                        'error': 'Stock not found',
                        'flutter_ready': True
                    }), 404
                data = [data]
            else:
                data = services['price_service'].get_all_stocks()
            
            market_status = services['price_service'].get_market_status()
            last_scrape = services['scraping_service'].get_last_scrape_time()
            
            return jsonify({
                'success': True,
                'data': data,
                'count': len(data),
                'market_status': market_status,
                'last_scrape': last_scrape.isoformat() if last_scrape else None,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True,
                'auth_info': {
                    'key_type': request.auth_info['key_type'],
                    'key_id': request.auth_info['key_id']
                }
            })
        except Exception as e:
            logger.error(f"Get stocks error: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/stocks/<symbol>', methods=['GET'])
    @require_auth
    def get_stock_by_symbol(symbol):
        """Get specific stock data by symbol"""
        try:
            price_service = app.config['price_service']
            data = price_service.get_stock_by_symbol(symbol)
            if data:
                return jsonify({
                    'success': True,
                    'data': data,
                    'market_status': price_service.get_market_status(),
                    'timestamp': datetime.now().isoformat(),
                    'flutter_ready': True
                })
            else:
                return jsonify({
                    'success': False,
                    'error': f'Stock {symbol.upper()} not found',
                    'flutter_ready': True
                }), 404
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/stocks/search', methods=['GET'])
    @require_auth
    def search_stocks():
        """Search stocks"""
        try:
            price_service = app.config['price_service']
            query = request.args.get('q', '').strip()
            if not query or len(query) < 2:
                return jsonify({
                    'success': False,
                    'error': 'Search query must be at least 2 characters',
                    'flutter_ready': True
                }), 400
            
            limit = min(int(request.args.get('limit', 20)), 100)
            results = price_service.search_stocks(query, limit)
            
            return jsonify({
                'success': True,
                'data': results,
                'count': len(results),
                'query': query,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/stocks/gainers', methods=['GET'])
    @require_auth
    def get_top_gainers():
        """Get top gaining stocks"""
        try:
            price_service = app.config['price_service']
            limit = min(int(request.args.get('limit', 10)), 50)
            gainers = price_service.get_top_gainers(limit)
            
            return jsonify({
                'success': True,
                'data': gainers,
                'count': len(gainers),
                'category': 'gainers',
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
    
    @app.route('/api/stocks/losers', methods=['GET'])
    @require_auth
    def get_top_losers():
        """Get top losing stocks"""
        try:
            price_service = app.config['price_service']
            limit = min(int(request.args.get('limit', 10)), 50)
            losers = price_service.get_top_losers(limit)
            
            return jsonify({
                'success': True,
                'data': losers,
                'count': len(losers),
                'category': 'losers',
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
    
    @app.route('/api/stocks/active', methods=['GET'])
    @require_auth
    def get_most_active():
        """Get most actively traded stocks"""
        try:
            price_service = app.config['price_service']
            limit = min(int(request.args.get('limit', 10)), 50)
            active = price_service.get_most_active(limit)
            
            return jsonify({
                'success': True,
                'data': active,
                'count': len(active),
                'category': 'active',
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
    
    @app.route('/api/market-summary', methods=['GET'])
    @require_auth
    def get_market_summary():
        """Get market summary statistics"""
        try:
            price_service = app.config['price_service']
            summary = price_service.get_market_summary()
            return jsonify({
                'success': True,
                'data': summary,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500