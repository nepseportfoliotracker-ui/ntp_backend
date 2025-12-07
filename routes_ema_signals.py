# routes_ema_signals.py - API Routes for EMA Signal Generator

from flask import jsonify, request
import logging

logger = logging.getLogger(__name__)


def register_ema_signal_routes(app):
    """Register EMA signal generation routes"""
    
    ema_signal_service = app.config.get('ema_signal_service')
    require_auth = app.config.get('require_auth')
    require_admin = app.config.get('require_admin')
    
    @app.route('/api/ema-signals/generate', methods=['POST'])
    @require_admin
    def generate_ema_signals():
        """
        Generate EMA trading signals (Admin only)
        
        Body (optional):
        {
            "force": true/false,
            "ema_period": 4,
            "min_holding_days": 2
        }
        """
        try:
            data = request.get_json() or {}
            force = data.get('force', False)
            
            # Allow custom parameters if provided
            ema_period = data.get('ema_period')
            min_holding_days = data.get('min_holding_days')
            
            if ema_period:
                ema_signal_service.ema_period = ema_period
            if min_holding_days:
                ema_signal_service.min_holding_days = min_holding_days
            
            result = ema_signal_service.generate_signals(force=force)
            
            if result['success']:
                return jsonify({
                    'success': True,
                    'message': 'EMA signals generated successfully',
                    'data': result
                }), 200
            else:
                return jsonify({
                    'success': False,
                    'error': result.get('error', 'Unknown error')
                }), 500
                
        except Exception as e:
            logger.error(f"Failed to generate EMA signals: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/latest', methods=['GET'])
    def get_latest_ema_signal():
        """
        Get the latest EMA trading signal (Public)
        
        Returns:
        {
            "success": true,
            "data": {
                "date": "2024-12-07",
                "signal": "BUY",
                "price": 2650.50,
                "ema": 2640.25,
                "can_trade": true,
                "holding_period_active": false,
                "holding_days_remaining": 0,
                "days_since_last_signal": 5
            }
        }
        """
        try:
            signal = ema_signal_service.get_latest_signal()
            
            if signal:
                return jsonify({
                    'success': True,
                    'data': signal
                }), 200
            else:
                return jsonify({
                    'success': False,
                    'error': 'No signals available'
                }), 404
                
        except Exception as e:
            logger.error(f"Failed to get latest signal: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/all', methods=['GET'])
    def get_all_ema_signals():
        """
        Get all EMA trading signals (Public)
        
        Query params:
        - limit: Number of signals to return (default: 100)
        
        Returns:
        {
            "success": true,
            "count": 50,
            "data": [...]
        }
        """
        try:
            limit = request.args.get('limit', 100, type=int)
            signals = ema_signal_service.get_all_signals(limit=limit)
            
            return jsonify({
                'success': True,
                'count': len(signals),
                'data': signals
            }), 200
                
        except Exception as e:
            logger.error(f"Failed to get all signals: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/date/<date>', methods=['GET'])
    def get_ema_signal_by_date(date):
        """
        Get EMA signal for a specific date (Public)
        
        Path param:
        - date: Date in YYYY-MM-DD format
        
        Returns:
        {
            "success": true,
            "data": {...}
        }
        """
        try:
            signal = ema_signal_service.get_signal_for_date(date)
            
            if signal:
                return jsonify({
                    'success': True,
                    'data': signal
                }), 200
            else:
                return jsonify({
                    'success': False,
                    'error': f'No signal found for date {date}'
                }), 404
                
        except Exception as e:
            logger.error(f"Failed to get signal for date: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/summary', methods=['GET'])
    def get_ema_trade_summary():
        """
        Get EMA trading performance summary (Public)
        
        Returns:
        {
            "success": true,
            "data": {
                "total_signals": 150,
                "buy_signals": 75,
                "sell_signals": 70,
                "hold_signals": 5,
                "total_trades": 35,
                "winning_trades": 20,
                "losing_trades": 15,
                "win_rate": 57.14,
                "avg_profit_loss": 1.25,
                "total_return": 43.75
            }
        }
        """
        try:
            summary = ema_signal_service.get_trade_summary()
            
            if summary:
                return jsonify({
                    'success': True,
                    'data': summary
                }), 200
            else:
                return jsonify({
                    'success': False,
                    'error': 'No trade summary available'
                }), 404
                
        except Exception as e:
            logger.error(f"Failed to get trade summary: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/parameters', methods=['GET'])
    def get_ema_parameters():
        """
        Get current EMA signal parameters (Public)
        
        Returns:
        {
            "success": true,
            "data": {
                "ema_period": 4,
                "min_holding_days": 2
            }
        }
        """
        try:
            return jsonify({
                'success': True,
                'data': {
                    'ema_period': ema_signal_service.ema_period,
                    'min_holding_days': ema_signal_service.min_holding_days
                }
            }), 200
                
        except Exception as e:
            logger.error(f"Failed to get parameters: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    logger.info("EMA signal routes registered successfully")