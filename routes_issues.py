# routes_issues.py - Issue Routes (IPO/FPO/Rights)

import logging
from datetime import datetime
from flask import jsonify, request

logger = logging.getLogger(__name__)


def register_issue_routes(app):
    """Register issue-related routes (IPO/FPO/Rights)"""
    
    # Get decorators from app config
    require_auth = app.config['require_auth']
    
    @app.route('/api/issues', methods=['GET'])
    @require_auth
    def get_all_issues():
        """Get all issues"""
        try:
            services = {
                'ipo_service': app.config['ipo_service'],
                'scraping_service': app.config['scraping_service']
            }
            
            status = request.args.get('status', 'all')
            category = request.args.get('category')
            limit = min(int(request.args.get('limit', 50)), 100)
            
            if status == 'open':
                data = services['ipo_service'].get_open_issues(category)
            elif status == 'coming_soon':
                data = services['ipo_service'].get_coming_soon_issues()
            else:
                all_issues = []
                all_issues.extend(services['ipo_service'].get_all_ipos())
                all_issues.extend(services['ipo_service'].get_all_fpos())
                all_issues.extend(services['ipo_service'].get_all_rights_dividends())
                
                if category:
                    category_upper = category.upper()
                    data = [issue for issue in all_issues if issue.get('issue_category', '').upper() == category_upper]
                else:
                    data = all_issues
                
                data.sort(key=lambda x: x.get('scraped_at', ''), reverse=True)
            
            data = data[:limit]
            
            return jsonify({
                'success': True,
                'data': data,
                'count': len(data),
                'filters': {
                    'status': status,
                    'category': category,
                    'limit': limit
                },
                'last_ipo_scrape': services['scraping_service'].get_last_ipo_scrape_time().isoformat() if services['scraping_service'].get_last_ipo_scrape_time() else None,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
            
        except Exception as e:
            logger.error(f"Get all issues error: {e}")
            return jsonify({
                'success': False,
                'error': str(e),
                'flutter_ready': True
            }), 500
    
    @app.route('/api/issues/ipos', methods=['GET'])
    @require_auth
    def get_ipos_only():
        """Get IPOs only"""
        try:
            ipo_service = app.config['ipo_service']
            data = ipo_service.get_all_ipos()
            return jsonify({
                'success': True,
                'data': data,
                'count': len(data),
                'category': 'IPO',
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
    
    @app.route('/api/issues/fpos', methods=['GET'])
    @require_auth
    def get_fpos_only():
        """Get FPOs only"""
        try:
            ipo_service = app.config['ipo_service']
            data = ipo_service.get_all_fpos()
            return jsonify({
                'success': True,
                'data': data,
                'count': len(data),
                'category': 'FPO',
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
    
    @app.route('/api/issues/rights', methods=['GET'])
    @require_auth
    def get_rights_only():
        """Get Rights/Dividends only"""
        try:
            ipo_service = app.config['ipo_service']
            data = ipo_service.get_all_rights_dividends()
            return jsonify({
                'success': True,
                'data': data,
                'count': len(data),
                'category': 'Rights',
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
    
    @app.route('/api/issues/open', methods=['GET'])
    @require_auth
    def get_open_issues():
        """Get currently open issues"""
        try:
            ipo_service = app.config['ipo_service']
            category = request.args.get('category')
            data = ipo_service.get_open_issues(category)
            
            return jsonify({
                'success': True,
                'data': data,
                'count': len(data),
                'status': 'open',
                'category_filter': category,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
    
    @app.route('/api/issues/coming-soon', methods=['GET'])
    @require_auth
    def get_coming_soon_issues():
        """Get coming soon issues"""
        try:
            ipo_service = app.config['ipo_service']
            data = ipo_service.get_coming_soon_issues()
            return jsonify({
                'success': True,
                'data': data,
                'count': len(data),
                'status': 'coming_soon',
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
    
    @app.route('/api/issues/search', methods=['GET'])
    @require_auth
    def search_issues():
        """Search all issues"""
        try:
            ipo_service = app.config['ipo_service']
            query = request.args.get('q', '').strip()
            if not query or len(query) < 2:
                return jsonify({
                    'success': False,
                    'error': 'Search query must be at least 2 characters',
                    'flutter_ready': True
                }), 400
            
            limit = min(int(request.args.get('limit', 20)), 100)
            results = ipo_service.search_issues(query, limit)
            
            return jsonify({
                'success': True,
                'data': results,
                'count': len(results),
                'query': query,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500
    
    @app.route('/api/issues/statistics', methods=['GET'])
    @require_auth
    def get_issue_statistics():
        """Get detailed statistics"""
        try:
            ipo_service = app.config['ipo_service']
            stats = ipo_service.get_statistics()
            return jsonify({
                'success': True,
                'statistics': stats,
                'timestamp': datetime.now().isoformat(),
                'flutter_ready': True
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'flutter_ready': True}), 500