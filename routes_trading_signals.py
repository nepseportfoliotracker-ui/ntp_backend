# routes_trading_signals.py - Trading Signals API Routes (Updated)

import logging
from flask import Blueprint, jsonify, request, current_app

logger = logging.getLogger(__name__)


def register_trading_signals_routes(app):
    """Register all trading signals routes"""
    
    @app.route('/api/v1/signals/generate', methods=['POST'])
    def generate_signals():
        """
        Generate trading signals from NEPSE historical data
        
        Query Parameters:
        - ema_period: EMA period (default: 20)
        - min_holding_days: Minimum holding days (default: 3)
        
        Returns:
        - latest_signal: Most recent signal
        - all_signals: All generated signals
        - trades: Trade performance statistics
        - metadata: Calculation metadata
        """
        try:
            signals_service = current_app.config['technical_signals_service']
            
            ema_period = request.args.get('ema_period', 20, type=int)
            min_holding = request.args.get('min_holding_days', 3, type=int)
            
            # Validate parameters
            if ema_period <= 0:
                return jsonify({'error': 'EMA period must be positive'}), 400
            if min_holding < 0:
                return jsonify({'error': 'Minimum holding days must be non-negative'}), 400
            
            result = signals_service.generate_signals(
                ema_period=ema_period,
                min_holding_days=min_holding
            )
            
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error generating signals: {e}")
            return jsonify({'error': 'Failed to generate signals', 'details': str(e)}), 500
    
    @app.route('/api/v1/signals/latest', methods=['GET'])
    def get_latest_signal():
        """
        Get the latest trading signal
        
        Returns:
        - date: Signal date
        - type: 'buy' or 'sell'
        - price: NEPSE index price at signal
        - ema: EMA value at signal
        - confidence: Confidence score (0-100)
        - days_since_last: Days since previous signal
        """
        try:
            signals_service = current_app.config['technical_signals_service']
            
            latest = signals_service.get_last_signal()
            
            if not latest:
                return jsonify({
                    'signal': None,
                    'message': 'No signals generated yet. Use POST /api/v1/signals/generate to create signals.'
                }), 200
            
            return jsonify({
                'signal': latest,
                'success': True
            })
            
        except Exception as e:
            logger.error(f"Error fetching latest signal: {e}")
            return jsonify({'error': 'Failed to fetch latest signal'}), 500
    
    @app.route('/api/v1/signals/history', methods=['GET'])
    def get_signal_history():
        """
        Get historical trading signals
        
        Query Parameters:
        - limit: Number of signals to return (default: 50, max: 500)
        
        Returns:
        - signals: Array of historical signals with metadata
        """
        try:
            signals_service = current_app.config['technical_signals_service']
            
            limit = request.args.get('limit', 50, type=int)
            limit = min(limit, 500)  # Cap at 500
            
            signals = signals_service.get_signals_history(limit=limit)
            
            return jsonify({
                'signals': signals,
                'count': len(signals),
                'success': True
            })
            
        except Exception as e:
            logger.error(f"Error fetching signal history: {e}")
            return jsonify({'error': 'Failed to fetch signal history'}), 500
    
    @app.route('/api/v1/signals/trades', methods=['GET'])
    def get_trades_history():
        """
        Get historical completed trades
        
        Query Parameters:
        - include_skipped: Include skipped trades (default: false)
        - limit: Number of trades to return (default: 50, max: 500)
        
        Returns:
        - trades: Array of completed trades
        - count: Total trades returned
        """
        try:
            signals_service = current_app.config['technical_signals_service']
            
            include_skipped = request.args.get('include_skipped', 'false').lower() == 'true'
            limit = request.args.get('limit', 50, type=int)
            limit = min(limit, 500)
            
            trades = signals_service.get_trades_history(
                include_skipped=include_skipped,
                limit=limit
            )
            
            return jsonify({
                'trades': trades,
                'count': len(trades),
                'includes_skipped': include_skipped,
                'success': True
            })
            
        except Exception as e:
            logger.error(f"Error fetching trades history: {e}")
            return jsonify({'error': 'Failed to fetch trades history'}), 500
    
    @app.route('/api/v1/signals/statistics', methods=['GET'])
    def get_signal_statistics():
        """
        Get overall trading signal and trade statistics
        
        Returns:
        - signals: Signal statistics (total, buy, sell, avg confidence)
        - trades: Trade performance (completed, wins, losses, win rate, returns)
        - skipped_trades: Skipped trade statistics
        """
        try:
            signals_service = current_app.config['technical_signals_service']
            
            stats = signals_service.get_signal_statistics()
            
            return jsonify({
                'statistics': stats,
                'success': True
            })
            
        except Exception as e:
            logger.error(f"Error fetching signal statistics: {e}")
            return jsonify({'error': 'Failed to fetch signal statistics'}), 500
    
    @app.route('/api/v1/signals/predict-tomorrow', methods=['GET'])
    def predict_tomorrow_signal():
        """
        Get prediction for tomorrow's signal based on current trend
        
        Returns:
        - predicted_signal: 'buy', 'sell', or 'hold'
        - current_price: Latest NEPSE price
        - ema: Current EMA value
        - trend: 'bullish', 'bearish', or 'neutral'
        - confidence: Prediction confidence (0-100)
        - reasoning: Explanation for the prediction
        """
        try:
            signals_service = current_app.config['technical_signals_service']
            nepse_history_service = current_app.config['nepse_history_service']
            
            latest_signal = signals_service.get_last_signal()
            
            if not latest_signal:
                return jsonify({
                    'error': 'No signals available for prediction. Generate signals first.'
                }), 400
            
            # Get latest NEPSE data
            latest_nepse = nepse_history_service.get_latest_data()
            
            if not latest_nepse:
                return jsonify({
                    'error': 'No NEPSE history data available'
                }), 400
            
            current_price = latest_nepse.get('index_value', latest_signal['price'])
            ema_value = latest_signal['ema']
            last_signal_type = latest_signal['type']
            confidence = latest_signal['confidence']
            
            # Determine trend and prediction
            price_vs_ema = current_price - ema_value
            
            if price_vs_ema > 0:
                trend = 'bullish'
                predicted_signal = 'buy' if last_signal_type == 'sell' else 'hold'
            elif price_vs_ema < 0:
                trend = 'bearish'
                predicted_signal = 'sell' if last_signal_type == 'buy' else 'hold'
            else:
                trend = 'neutral'
                predicted_signal = 'hold'
            
            # Build reasoning
            reasoning = []
            
            if trend == 'bullish':
                reasoning.append(f"Price ({current_price:.2f}) is above EMA ({ema_value:.2f}) - bullish signal")
                if last_signal_type == 'buy':
                    reasoning.append("Already in buy position - hold and monitor")
                else:
                    reasoning.append("Price crossed above EMA - potential buy opportunity")
            elif trend == 'bearish':
                reasoning.append(f"Price ({current_price:.2f}) is below EMA ({ema_value:.2f}) - bearish signal")
                if last_signal_type == 'sell':
                    reasoning.append("Already in sell position - stay out of market")
                else:
                    reasoning.append("Price crossed below EMA - potential sell signal")
            else:
                reasoning.append("Price at EMA - neutral/wait for clear direction")
            
            # Add momentum info
            days_since = latest_signal.get('days_since_last', 0)
            if days_since:
                reasoning.append(f"Last signal was {days_since} days ago")
            
            return jsonify({
                'prediction': {
                    'signal': predicted_signal,
                    'trend': trend,
                    'confidence': round(confidence, 1),
                    'current_price': round(current_price, 2),
                    'ema': round(ema_value, 2),
                    'price_vs_ema': round(price_vs_ema, 2),
                    'price_vs_ema_pct': round((price_vs_ema / ema_value * 100), 2),
                    'reasoning': reasoning,
                    'last_signal': {
                        'type': last_signal_type,
                        'date': latest_signal['date'],
                        'price': latest_signal['price']
                    }
                },
                'success': True
            })
            
        except Exception as e:
            logger.error(f"Error predicting tomorrow's signal: {e}")
            return jsonify({'error': 'Failed to predict tomorrow\'s signal'}), 500
    
    @app.route('/api/v1/signals/analysis', methods=['GET'])
    def get_signal_analysis():
        """
        Get detailed analysis of trading signals and performance
        
        Returns:
        - signal_overview: Signal statistics
        - trade_performance: Trade win/loss analysis
        - current_state: Latest signal and trend
        - recommendation: Trading recommendation
        """
        try:
            signals_service = current_app.config['technical_signals_service']
            
            stats = signals_service.get_signal_statistics()
            latest = signals_service.get_last_signal()
            
            if not stats or not latest:
                return jsonify({
                    'error': 'Insufficient data for analysis. Generate signals first.',
                    'success': False
                }), 400
            
            # Calculate additional metrics
            signal_stats = stats.get('signals', {})
            trade_stats = stats.get('trades', {})
            skipped_stats = stats.get('skipped_trades', {})
            
            # Determine current trend
            current_trend = 'unknown'
            if latest:
                if latest['price'] > latest['ema']:
                    current_trend = 'bullish'
                elif latest['price'] < latest['ema']:
                    current_trend = 'bearish'
                else:
                    current_trend = 'neutral'
            
            # Build recommendation
            recommendation = "Monitor market for next signal"
            if latest:
                if latest['type'] == 'buy' and current_trend == 'bullish':
                    recommendation = "Consider holding position - uptrend continues"
                elif latest['type'] == 'sell' and current_trend == 'bearish':
                    recommendation = "Stay out of market - downtrend continues"
                elif latest['type'] == 'buy' and current_trend == 'bearish':
                    recommendation = "Watch for exit signal - trend may be reversing"
                elif latest['type'] == 'sell' and current_trend == 'bullish':
                    recommendation = "Watch for entry signal - trend may be reversing"
            
            analysis = {
                'signal_overview': {
                    'total_signals': signal_stats.get('total', 0),
                    'buy_signals': signal_stats.get('buy', 0),
                    'sell_signals': signal_stats.get('sell', 0),
                    'average_confidence': signal_stats.get('avg_confidence', 0)
                },
                'trade_performance': {
                    'completed_trades': trade_stats.get('completed', 0),
                    'winning_trades': trade_stats.get('wins', 0),
                    'losing_trades': trade_stats.get('losses', 0),
                    'win_rate_percent': trade_stats.get('win_rate', 0),
                    'total_return_percent': trade_stats.get('total_return', 0),
                    'average_return_per_trade': trade_stats.get('avg_return', 0),
                    'average_days_held': trade_stats.get('avg_days_held', 0)
                },
                'skipped_trades': {
                    'count': skipped_stats.get('count', 0),
                    'total_return_percent': skipped_stats.get('total_return', 0)
                },
                'current_state': {
                    'latest_signal': latest,
                    'trend': current_trend,
                    'days_since_last_signal': latest.get('days_since_last', 0) if latest else 0
                },
                'recommendation': recommendation
            }
            
            return jsonify({
                'analysis': analysis,
                'success': True
            })
            
        except Exception as e:
            logger.error(f"Error getting signal analysis: {e}")
            return jsonify({'error': 'Failed to get signal analysis'}), 500
    
    @app.route('/api/v1/signals/backtest', methods=['GET'])
    def get_backtest_summary():
        """
        Get backtest summary with all trade details
        
        Returns:
        - Comprehensive backtest results including all trades
        """
        try:
            signals_service = current_app.config['technical_signals_service']
            
            # Get all data
            stats = signals_service.get_signal_statistics()
            all_trades = signals_service.get_trades_history(include_skipped=True, limit=500)
            all_signals = signals_service.get_signals_history(limit=500)
            
            # Separate completed and skipped
            completed_trades = [t for t in all_trades if not t['was_skipped']]
            skipped_trades = [t for t in all_trades if t['was_skipped']]
            
            backtest = {
                'summary': stats,
                'signals': {
                    'total': len(all_signals),
                    'data': all_signals
                },
                'completed_trades': {
                    'count': len(completed_trades),
                    'data': completed_trades
                },
                'skipped_trades': {
                    'count': len(skipped_trades),
                    'data': skipped_trades
                },
                'success': True
            }
            
            return jsonify(backtest)
            
        except Exception as e:
            logger.error(f"Error getting backtest summary: {e}")
            return jsonify({'error': 'Failed to get backtest summary'}), 500