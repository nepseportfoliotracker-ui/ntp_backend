# routes_ema_signals.py - UPDATED API Routes for EMA Signal Generator

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
        Get the latest EMA CROSSOVER signal (actionable trading signal)
        This returns the last time price crossed above/below EMA
        
        Returns:
        {
            "success": true,
            "data": {
                "date": "2025-11-30",
                "signal": "SELL",
                "price": 2649.52,
                "ema": 2653.06,
                "can_trade": true,
                "holding_period_active": false,
                "holding_days_remaining": 0,
                "days_since_last_signal": 13
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
    
    @app.route('/api/ema-signals/current-status', methods=['GET'])
    def get_current_market_status():
        """
        Get CURRENT market status (today's price vs EMA)
        This shows the relationship between current price and EMA
        
        Returns:
        {
            "success": true,
            "data": {
                "date": "2025-12-16",
                "price": 2618.30,
                "ema": 2609.15,
                "price_ema_diff": 9.15,
                "price_ema_diff_percent": 0.35,
                "position": "ABOVE_EMA",
                "position_text": "Price is above EMA (Bullish)",
                "last_signal_type": "HOLD",
                "can_trade": false,
                "holding_period_active": false,
                "holding_days_remaining": 0,
                "days_since_last_signal": 16,
                "is_crossover": false
            }
        }
        """
        try:
            status = ema_signal_service.get_current_market_status()
            
            if status:
                return jsonify({
                    'success': True,
                    'data': status
                }), 200
            else:
                return jsonify({
                    'success': False,
                    'error': 'No market status available'
                }), 404
                
        except Exception as e:
            logger.error(f"Failed to get current market status: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/all', methods=['GET'])
    def get_all_ema_signals():
        """
        Get all EMA trading signals
        
        Query params:
        - limit: Number of signals to return (default: 100)
        - crossovers_only: If true, only return crossover signals (default: false)
        
        Returns:
        {
            "success": true,
            "count": 50,
            "data": [...]
        }
        """
        try:
            limit = request.args.get('limit', 100, type=int)
            crossovers_only = request.args.get('crossovers_only', 'false').lower() == 'true'
            
            signals = ema_signal_service.get_all_signals(limit=limit, crossovers_only=crossovers_only)
            
            return jsonify({
                'success': True,
                'count': len(signals),
                'crossovers_only': crossovers_only,
                'data': signals
            }), 200
                
        except Exception as e:
            logger.error(f"Failed to get all signals: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/crossovers', methods=['GET'])
    def get_crossover_signals():
        """
        Get only crossover signals (BUY/SELL events)
        Convenience endpoint - same as /all?crossovers_only=true
        
        Query params:
        - limit: Number of signals to return (default: 50)
        
        Returns:
        {
            "success": true,
            "count": 37,
            "data": [...]
        }
        """
        try:
            limit = request.args.get('limit', 50, type=int)
            signals = ema_signal_service.get_all_signals(limit=limit, crossovers_only=True)
            
            return jsonify({
                'success': True,
                'count': len(signals),
                'data': signals
            }), 200
                
        except Exception as e:
            logger.error(f"Failed to get crossover signals: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ema-signals/date/<date>', methods=['GET'])
    def get_ema_signal_by_date(date):
        """
        Get EMA signal for a specific date
        
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
        Get EMA trading performance summary
        
        Returns:
        {
            "success": true,
            "data": {
                "total_signals": 41,
                "buy_signals": 20,
                "sell_signals": 21,
                "hold_signals": 0,
                "total_trades": 16,
                "winning_trades": 12,
                "losing_trades": 4,
                "win_rate": 75.0,
                "avg_profit_loss": 0.82,
                "total_return": 13.15
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
        Get current EMA signal parameters
        
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
    
    @app.route('/api/ema-signals/dashboard', methods=['GET'])
    def get_ema_dashboard():
        """
        Get complete EMA trading dashboard data
        Combines current status, latest signal, and trade summary
        
        Returns:
        {
            "success": true,
            "data": {
                "current_status": {...},
                "latest_signal": {...},
                "trade_summary": {...},
                "parameters": {...}
            }
        }
        """
        try:
            current_status = ema_signal_service.get_current_market_status()
            latest_signal = ema_signal_service.get_latest_signal()
            trade_summary = ema_signal_service.get_trade_summary()
            
            return jsonify({
                'success': True,
                'data': {
                    'current_status': current_status,
                    'latest_signal': latest_signal,
                    'trade_summary': trade_summary,
                    'parameters': {
                        'ema_period': ema_signal_service.ema_period,
                        'min_holding_days': ema_signal_service.min_holding_days
                    }
                }
            }), 200
                
        except Exception as e:
            logger.error(f"Failed to get dashboard data: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    logger.info("EMA signal routes registered successfully")