# routes_market_overview.py - API endpoints for market overview data

from flask import Blueprint, jsonify, request, current_app
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Create blueprint
market_overview_bp = Blueprint('market_overview', __name__, url_prefix='/api/market-overview')

# Helper to get market overview service
def get_overview_service():
    return current_app.config.get('market_overview_service')


@market_overview_bp.route('/latest', methods=['GET'])
def get_latest_overview():
    """
    Get latest market overview snapshot
    
    Returns:
        {
            "status": "success",
            "data": {
                "snapshot_id": 123,
                "timestamp": "2024-01-15T11:30:00",
                "data": {
                    "total_stocks": 250,
                    "active_stocks": 248,
                    "top_gainers": [...],
                    "top_losers": [...],
                    "top_active_quantity": [...],
                    "top_active_turnover": [...],
                    "market_stats": {
                        "advancing": 180,
                        "declining": 65,
                        "unchanged": 5,
                        "total_turnover": 1500000000,
                        "total_volume": 50000000
                    }
                }
            }
        }
    """
    try:
        service = get_overview_service()
        overview = service.get_latest_overview()
        
        if not overview:
            return jsonify({
                'status': 'error',
                'message': 'No market overview data available'
            }), 404
        
        return jsonify({
            'status': 'success',
            'data': overview
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching latest overview: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@market_overview_bp.route('/top-gainers', methods=['GET'])
def get_top_gainers():
    """Get latest top gainers list"""
    try:
        limit = request.args.get('limit', 10, type=int)
        limit = min(limit, 20)  # Cap at 20
        
        service = get_overview_service()
        overview = service.get_latest_overview()
        
        if not overview:
            return jsonify({
                'status': 'error',
                'message': 'No data available'
            }), 404
        
        gainers = overview['data']['top_gainers'][:limit]
        
        return jsonify({
            'status': 'success',
            'timestamp': overview['timestamp'],
            'count': len(gainers),
            'data': gainers
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching top gainers: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@market_overview_bp.route('/top-losers', methods=['GET'])
def get_top_losers():
    """Get latest top losers list"""
    try:
        limit = request.args.get('limit', 10, type=int)
        limit = min(limit, 20)
        
        service = get_overview_service()
        overview = service.get_latest_overview()
        
        if not overview:
            return jsonify({
                'status': 'error',
                'message': 'No data available'
            }), 404
        
        losers = overview['data']['top_losers'][:limit]
        
        return jsonify({
            'status': 'success',
            'timestamp': overview['timestamp'],
            'count': len(losers),
            'data': losers
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching top losers: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@market_overview_bp.route('/top-active', methods=['GET'])
def get_top_active():
    """
    Get top active stocks
    
    Query params:
        - sort_by: 'quantity' or 'turnover' (default: turnover)
        - limit: number of items (default: 10)
    """
    try:
        sort_by = request.args.get('sort_by', 'turnover', type=str)
        limit = request.args.get('limit', 10, type=int)
        limit = min(limit, 20)
        
        service = get_overview_service()
        overview = service.get_latest_overview()
        
        if not overview:
            return jsonify({
                'status': 'error',
                'message': 'No data available'
            }), 404
        
        if sort_by == 'quantity':
            active = overview['data']['top_active_quantity'][:limit]
            sort_label = 'By Quantity'
        else:
            active = overview['data']['top_active_turnover'][:limit]
            sort_label = 'By Turnover'
        
        return jsonify({
            'status': 'success',
            'timestamp': overview['timestamp'],
            'sort_by': sort_label,
            'count': len(active),
            'data': active
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching top active: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@market_overview_bp.route('/market-stats', methods=['GET'])
def get_market_stats():
    """Get current market statistics"""
    try:
        service = get_overview_service()
        overview = service.get_latest_overview()
        
        if not overview:
            return jsonify({
                'status': 'error',
                'message': 'No data available'
            }), 404
        
        stats = overview['data']['market_stats']
        stats['timestamp'] = overview['timestamp']
        stats['total_stocks'] = overview['data']['total_stocks']
        stats['active_stocks'] = overview['data']['active_stocks']
        
        return jsonify({
            'status': 'success',
            'data': stats
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching market stats: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@market_overview_bp.route('/history', methods=['GET'])
def get_overview_history():
    """
    Get overview history within time range
    
    Query params:
        - hours: last N hours (default: 24)
        - limit: max snapshots (default: 50)
    """
    try:
        hours = request.args.get('hours', 24, type=int)
        limit = request.args.get('limit', 50, type=int)
        limit = min(limit, 200)
        
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        service = get_overview_service()
        history = service.get_overview_history(start_time, end_time, limit)
        
        return jsonify({
            'status': 'success',
            'count': len(history),
            'time_range': {
                'start': start_time.isoformat(),
                'end': end_time.isoformat(),
                'hours': hours
            },
            'data': history
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching overview history: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@market_overview_bp.route('/daily-summary', methods=['GET'])
def get_daily_summary():
    """
    Get daily overview summary
    
    Query params:
        - date: YYYY-MM-DD (default: today)
    """
    try:
        date_str = request.args.get('date', None, type=str)
        
        if date_str:
            try:
                date = datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'message': 'Invalid date format. Use YYYY-MM-DD'
                }), 400
        else:
            date = None
        
        service = get_overview_service()
        summary = service.get_daily_summary(date)
        
        if not summary:
            return jsonify({
                'status': 'error',
                'message': 'No daily summary available for this date'
            }), 404
        
        return jsonify({
            'status': 'success',
            'data': summary
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching daily summary: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@market_overview_bp.route('/comparisons', methods=['GET'])
def get_market_comparison():
    """
    Get comparison data showing how market is performing
    
    Returns gainers vs losers, advancing vs declining count
    """
    try:
        service = get_overview_service()
        overview = service.get_latest_overview()
        
        if not overview:
            return jsonify({
                'status': 'error',
                'message': 'No data available'
            }), 404
        
        data = overview['data']
        stats = data['market_stats']
        
        # Calculate performance metrics
        total_active = stats['advancing'] + stats['declining']
        advancing_pct = (stats['advancing'] / total_active * 100) if total_active > 0 else 0
        declining_pct = (stats['declining'] / total_active * 100) if total_active > 0 else 0
        
        comparison = {
            'timestamp': overview['timestamp'],
            'market_sentiment': 'Bullish' if advancing_pct > 50 else 'Bearish' if declining_pct > 50 else 'Neutral',
            'advancing': {
                'count': stats['advancing'],
                'percentage': round(advancing_pct, 2)
            },
            'declining': {
                'count': stats['declining'],
                'percentage': round(declining_pct, 2)
            },
            'unchanged': stats['unchanged'],
            'total_stocks': data['total_stocks'],
            'turnover': {
                'value': stats['total_turnover'],
                'formatted': f"{stats['total_turnover'] / 1e7:.2f} Cr"
            },
            'volume': {
                'value': stats['total_volume'],
                'formatted': f"{stats['total_volume'] / 1e6:.2f} M"
            },
            'top_gainer': data['top_gainers'][0] if data['top_gainers'] else None,
            'top_loser': data['top_losers'][0] if data['top_losers'] else None
        }
        
        return jsonify({
            'status': 'success',
            'data': comparison
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching market comparison: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


def register_market_overview_routes(app):
    """Register market overview blueprint with app"""
    app.register_blueprint(market_overview_bp)
    logger.info("Market overview routes registered")