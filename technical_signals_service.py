# technical_signals_service.py - NEPSE Trading Signals Generator (Fixed)

import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import numpy as np

logger = logging.getLogger(__name__)


class TechnicalSignalsService:
    """Service for generating EMA-based trading signals for NEPSE index"""
    
    def __init__(self, db_service, nepse_history_service):
        self.db_service = db_service
        self.nepse_history_service = nepse_history_service
        self._init_signals_table()
        self._init_trades_table()
    
    def _init_signals_table(self):
        """Initialize table to store trading signals"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS nepse_trading_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_date DATE NOT NULL UNIQUE,
                    signal_type TEXT NOT NULL,
                    current_price REAL NOT NULL,
                    ema_value REAL NOT NULL,
                    price_ema_diff REAL NOT NULL,
                    days_since_last_signal INTEGER,
                    metadata TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_signal_date ON nepse_trading_signals(signal_date DESC)')
            conn.commit()
            logger.info("Trading signals table initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize signals table: {e}")
        finally:
            conn.close()
    
    def _init_trades_table(self):
        """Initialize table to store completed trades"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS nepse_completed_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_date DATE NOT NULL,
                    entry_price REAL NOT NULL,
                    exit_date DATE NOT NULL,
                    exit_price REAL NOT NULL,
                    return_pct REAL NOT NULL,
                    days_held INTEGER NOT NULL,
                    result TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_dates ON nepse_completed_trades(entry_date, exit_date)')
            conn.commit()
            logger.info("Completed trades table initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize trades table: {e}")
        finally:
            conn.close()
    
    def calculate_ema(self, prices: pd.Series, period: int) -> pd.Series:
        """Calculate Exponential Moving Average"""
        return prices.ewm(span=period, adjust=False).mean()
    
    def detect_price_ema_crossovers(self, price: pd.Series, ema: pd.Series) -> List[Dict]:
        """
        Detect price-EMA crossover signals
        Price crosses above EMA = Buy Signal
        Price crosses below EMA = Sell Signal
        """
        signals = []
        
        for i in range(1, len(price)):
            # Buy signal: Price crosses above EMA
            if price.iloc[i] > ema.iloc[i] and price.iloc[i-1] <= ema.iloc[i-1]:
                signals.append({
                    'index': i,
                    'type': 'buy',
                    'price': price.iloc[i],
                    'ema': ema.iloc[i]
                })
            
            # Sell signal: Price crosses below EMA
            elif price.iloc[i] < ema.iloc[i] and price.iloc[i-1] >= ema.iloc[i-1]:
                signals.append({
                    'index': i,
                    'type': 'sell',
                    'price': price.iloc[i],
                    'ema': ema.iloc[i]
                })
        
        return signals
    
    def calculate_returns_from_signals(self, df: pd.DataFrame, signals: List[Dict], 
                                      min_holding_days: int = 3) -> Dict:
        """
        Calculate returns from trading on signals with minimum holding period
        
        Rules:
        - Enter on BUY signal
        - IGNORE all signals (buy or sell) until min_holding_days have passed
        - Exit on first SELL signal AFTER min_holding_days
        - Win if exit_price > entry_price
        """
        trades = []
        ignored_signals = []  # Track signals that were ignored
        position = None
        entry_price = None
        entry_date = None
        entry_idx = None
        
        logger.info("\n=== PROCESSING SIGNALS ===")
        
        for signal in signals:
            idx = signal['index']
            price = df['index_value'].iloc[idx]
            signal_type = signal['type']
            date = df['date'].iloc[idx]
            
            # Debug each signal
            pos_status = f"OPEN since {entry_date.date() if entry_date else 'N/A'}" if position else "CLOSED"
            logger.debug(f"{date.date()} {signal_type.upper():4s} | Position: {pos_status}")
            
            # No position - only BUY signals matter
            if position is None:
                if signal_type == 'buy':
                    # Open long position
                    position = 'long'
                    entry_price = price
                    entry_date = date
                    entry_idx = idx
                else:
                    # SELL signal with no position - ignore
                    ignored_signals.append({
                        'date': date,
                        'type': signal_type,
                        'price': price,
                        'reason': 'No open position'
                    })
            
            # Position is open
            else:
                days_held = idx - entry_idx
                
                # Haven't met minimum holding period yet
                if days_held < min_holding_days:
                    ignored_signals.append({
                        'date': date,
                        'type': signal_type,
                        'price': price,
                        'reason': f'Position held only {days_held} days (min: {min_holding_days})'
                    })
                    continue  # Ignore this signal completely
                
                # Met minimum holding period
                if signal_type == 'sell':
                    # Valid SELL - close position
                    ret = (price - entry_price) / entry_price * 100
                    
                    trades.append({
                        'entry_date': entry_date,
                        'entry_price': entry_price,
                        'exit_date': date,
                        'exit_price': price,
                        'return': ret,
                        'days_held': days_held,
                        'result': 'WIN' if ret > 0 else 'LOSS'
                    })
                    
                    logger.debug(f"  → TRADE COMPLETED: {entry_date.date()} to {date.date()} ({days_held} days, {ret:.2f}%)")
                    
                    # Close position
                    position = None
                    entry_price = None
                    entry_date = None
                    entry_idx = None
                    
                elif signal_type == 'buy':
                    # BUY signal while position is open - ignore it
                    ignored_signals.append({
                        'date': date,
                        'type': signal_type,
                        'price': price,
                        'reason': f'Position already open since {entry_date.date()}'
                    })
        
        # Calculate statistics
        if len(trades) > 0:
            total_return = sum([t['return'] for t in trades])
            winning_trades = len([t for t in trades if t['return'] > 0])
            losing_trades = len([t for t in trades if t['return'] <= 0])
            avg_return = total_return / len(trades)
            avg_days_held = sum([t['days_held'] for t in trades]) / len(trades)
            
            winning_returns = [t['return'] for t in trades if t['return'] > 0]
            losing_returns = [t['return'] for t in trades if t['return'] <= 0]
            
            avg_winning_trade = sum(winning_returns) / len(winning_returns) if winning_returns else 0
            avg_losing_trade = sum(losing_returns) / len(losing_returns) if losing_returns else 0
            win_rate = (winning_trades / len(trades)) * 100
        else:
            total_return = 0
            winning_trades = 0
            losing_trades = 0
            avg_return = 0
            avg_winning_trade = 0
            avg_losing_trade = 0
            avg_days_held = 0
            win_rate = 0
        
        # Calculate ignored signals stats
        ignored_stats = None
        if ignored_signals:
            ignored_stats = {
                'count': len(ignored_signals),
                'buy_signals_ignored': len([s for s in ignored_signals if s['type'] == 'buy']),
                'sell_signals_ignored': len([s for s in ignored_signals if s['type'] == 'sell']),
                'reasons': {}
            }
            # Group by reason
            for sig in ignored_signals:
                reason = sig['reason']
                if reason not in ignored_stats['reasons']:
                    ignored_stats['reasons'][reason] = 0
                ignored_stats['reasons'][reason] += 1
        
        return {
            'trades': trades,
            'ignored_signals': ignored_signals,
            'total_return': total_return,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'avg_return': avg_return,
            'avg_winning_trade': avg_winning_trade,
            'avg_losing_trade': avg_losing_trade,
            'avg_days_held': avg_days_held,
            'win_rate': win_rate,
            'ignored_stats': ignored_stats
        }
    
    def save_signal(self, signal_date: str, signal_type: str, current_price: float,
                   ema_value: float, days_since: int = None,
                   metadata: str = None) -> bool:
        """Save trading signal to database"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            price_ema_diff = current_price - ema_value
            
            cursor.execute('''
                INSERT OR REPLACE INTO nepse_trading_signals
                (signal_date, signal_type, current_price, ema_value, 
                 price_ema_diff, days_since_last_signal, metadata, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                signal_date, signal_type, current_price, ema_value,
                price_ema_diff, days_since, metadata
            ))
            
            conn.commit()
            logger.info(f"Saved {signal_type.upper()} signal for {signal_date}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving signal: {e}")
            return False
        finally:
            conn.close()
    
    def save_trade(self, trade_data: Dict) -> bool:
        """Save completed trade to database"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO nepse_completed_trades
                (entry_date, entry_price, exit_date, exit_price, return_pct, 
                 days_held, result)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade_data['entry_date'].date().isoformat() if hasattr(trade_data['entry_date'], 'date') else str(trade_data['entry_date']),
                trade_data['entry_price'],
                trade_data['exit_date'].date().isoformat() if hasattr(trade_data['exit_date'], 'date') else str(trade_data['exit_date']),
                trade_data['exit_price'],
                trade_data['return'],
                trade_data['days_held'],
                trade_data['result']
            ))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error saving trade: {e}")
            return False
        finally:
            conn.close()
    
    def get_last_signal(self) -> Optional[Dict]:
        """Get the last trading signal from database"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT signal_date, signal_type, current_price, ema_value, days_since_last_signal
                FROM nepse_trading_signals
                ORDER BY signal_date DESC
                LIMIT 1
            ''')
            
            result = cursor.fetchone()
            if result:
                return {
                    'date': str(result[0]),
                    'type': result[1],
                    'price': result[2],
                    'ema': result[3],
                    'days_since_last': result[4]
                }
            return None
            
        except Exception as e:
            logger.error(f"Error fetching last signal: {e}")
            return None
        finally:
            conn.close()
    
    def generate_signals(self, ema_period: int = 3, min_holding_days: int = 3) -> Dict:
        """
        Generate trading signals from NEPSE historical data
        Pure crossover-based signals with minimum holding period enforcement
        
        Returns:
            Dict with:
            - latest_signal: Most recent valid signal
            - all_signals: List of valid signals that resulted in trades
            - trades: Completed trades analysis
            - metadata: Information about calculation
        """
        try:
            logger.info("=== Generating NEPSE Trading Signals (Pure Crossover with Min Holding) ===")
            logger.info(f"Parameters: EMA({ema_period}), Min Holding: {min_holding_days} days")
            
            # Get historical data
            history_data = self.nepse_history_service._get_data_from_table('yearly')
            
            if not history_data or len(history_data) < ema_period + 10:
                logger.warning("Insufficient historical data for signal generation")
                return {
                    'success': False,
                    'error': 'Insufficient historical data',
                    'latest_signal': None,
                    'all_signals': [],
                    'trades': None
                }
            
            # Convert to DataFrame
            df = pd.DataFrame(history_data)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            logger.info(f"Processing {len(df)} historical data points")
            logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
            
            # Calculate EMA
            df['ema'] = self.calculate_ema(df['index_value'], ema_period)
            
            # Detect all crossovers
            all_crossovers = self.detect_price_ema_crossovers(df['index_value'], df['ema'])
            logger.info(f"Detected {len(all_crossovers)} total crossover points")
            
            buy_count = len([s for s in all_crossovers if s['type'] == 'buy'])
            sell_count = len([s for s in all_crossovers if s['type'] == 'sell'])
            logger.info(f"Buy crossovers: {buy_count}, Sell crossovers: {sell_count}")
            
            # Calculate returns using backtest methodology with min holding
            trade_analysis = self.calculate_returns_from_signals(df, all_crossovers, min_holding_days)
            
            logger.info(f"\n=== TRADING PERFORMANCE (MIN {min_holding_days} DAYS HOLDING) ===")
            logger.info(f"Completed trades: {len(trade_analysis['trades'])}")
            logger.info(f"Winning trades: {trade_analysis['winning_trades']}")
            logger.info(f"Losing trades: {trade_analysis['losing_trades']}")
            logger.info(f"Win Rate: {trade_analysis['win_rate']:.2f}%")
            logger.info(f"Total return: {trade_analysis['total_return']:.2f}%")
            logger.info(f"Average return per trade: {trade_analysis['avg_return']:.2f}%")
            logger.info(f"Average days held: {trade_analysis['avg_days_held']:.1f} days")
            
            if trade_analysis['ignored_stats']:
                logger.info(f"\n=== IGNORED SIGNALS (WITHIN {min_holding_days} DAYS OR NO POSITION) ===")
                logger.info(f"Total ignored: {trade_analysis['ignored_stats']['count']}")
                logger.info(f"Buy signals ignored: {trade_analysis['ignored_stats']['buy_signals_ignored']}")
                logger.info(f"Sell signals ignored: {trade_analysis['ignored_stats']['sell_signals_ignored']}")
                logger.info(f"Breakdown by reason:")
                for reason, count in trade_analysis['ignored_stats']['reasons'].items():
                    logger.info(f"  - {reason}: {count}")
            
            # Clear existing signals and trades
            conn = self.db_service.get_connection()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM nepse_trading_signals')
            cursor.execute('DELETE FROM nepse_completed_trades')
            conn.commit()
            conn.close()
            
            # Get valid signal indices
            # Strategy: Save signals that represent the true current market state
            # 1) All signals from completed trades (both entry and exit)
            # 2) The absolute most recent crossover (even if ignored due to open position)
            #    This shows what the market is currently doing
            
            valid_signal_indices = set()
            
            # Add signals from completed trades
            for trade in trade_analysis['trades']:
                for signal in all_crossovers:
                    signal_date = df['date'].iloc[signal['index']]
                    if signal_date == trade['entry_date'] or signal_date == trade['exit_date']:
                        valid_signal_indices.add(signal['index'])
            
            logger.info(f"Signals from completed trades: {len(valid_signal_indices)}")
            
            # CRITICAL FIX: Add the absolute most recent crossover
            # This shows the current market state, even if we can't act on it yet
            if all_crossovers:
                most_recent_crossover = all_crossovers[-1]
                most_recent_idx = most_recent_crossover['index']
                most_recent_date = df['date'].iloc[most_recent_idx]
                most_recent_date_str = most_recent_date.date() if hasattr(most_recent_date, 'date') else most_recent_date
                
                # Check if it was ignored
                was_ignored = False
                ignore_reason = None
                for ignored in trade_analysis['ignored_signals']:
                    ignored_date = ignored['date']
                    if hasattr(ignored_date, 'date'):
                        ignored_date = ignored_date.date()
                    
                    if ignored_date == most_recent_date_str and ignored['type'] == most_recent_crossover['type']:
                        was_ignored = True
                        ignore_reason = ignored['reason']
                        break
                
                # Add it regardless of whether it was ignored
                valid_signal_indices.add(most_recent_idx)
                
                if was_ignored:
                    logger.info(f"Most recent crossover: {most_recent_crossover['type'].upper()} on {most_recent_date_str}")
                    logger.info(f"  (Was ignored: {ignore_reason}, but saving as current market state)")
                else:
                    logger.info(f"Most recent crossover: {most_recent_crossover['type'].upper()} on {most_recent_date_str} (actionable)")
            
            logger.info(f"Total valid signals to save: {len(valid_signal_indices)}")
            
            # Save all VALID signals
            saved_signals = []
            previous_signal_date = None
            
            # Determine if the most recent signal is in an open position
            most_recent_signal_idx = all_crossovers[-1]['index'] if all_crossovers else None
            is_open_position = False
            
            if most_recent_signal_idx:
                for ignored in trade_analysis['ignored_signals']:
                    ignored_date = ignored['date']
                    most_recent_date = df['date'].iloc[most_recent_signal_idx]
                    
                    if hasattr(ignored_date, 'date'):
                        ignored_date = ignored_date.date()
                    if hasattr(most_recent_date, 'date'):
                        most_recent_date = most_recent_date.date()
                    
                    if (ignored_date == most_recent_date and 
                        ignored['type'] == all_crossovers[-1]['type'] and
                        'Position already open' in ignored['reason']):
                        is_open_position = True
                        break
            
            for signal in all_crossovers:
                if signal['index'] not in valid_signal_indices:
                    continue  # Skip invalid/ignored signals
                
                idx = signal['index']
                signal_date = df['date'].iloc[idx].date().isoformat()
                signal_type = signal['type']
                current_price = signal['price']
                ema_value = signal['ema']
                
                # Calculate days since last signal
                days_since = None
                if previous_signal_date:
                    days_since = (df['date'].iloc[idx] - previous_signal_date).days
                
                # Determine metadata
                metadata_parts = [f"EMA({ema_period})", f"MinHold:{min_holding_days}d"]
                
                # Mark if this is the most recent signal and its state
                if signal['index'] == most_recent_signal_idx:
                    if is_open_position:
                        metadata_parts.append("OPEN_POSITION")
                    else:
                        metadata_parts.append("CURRENT")
                
                # Save signal
                self.save_signal(
                    signal_date=signal_date,
                    signal_type=signal_type,
                    current_price=current_price,
                    ema_value=ema_value,
                    days_since=days_since,
                    metadata="|".join(metadata_parts)
                )
                
                saved_signals.append({
                    'date': signal_date,
                    'type': signal_type,
                    'price': round(current_price, 2),
                    'ema': round(ema_value, 2),
                    'days_since_last': days_since
                })
                
                previous_signal_date = df['date'].iloc[idx]
            
            # Save completed trades
            for trade in trade_analysis['trades']:
                self.save_trade(trade)
            
            # Get latest signal
            latest_signal = self.get_last_signal()
            
            logger.info(f"\nSignal generation completed: {len(saved_signals)} valid signals saved")
            logger.info(f"({len(all_crossovers) - len(saved_signals)} signals ignored)")
            if latest_signal:
                logger.info(f"Latest valid signal: {latest_signal['type'].upper()} on {latest_signal['date']}")
            
            # DEBUG: Show last 10 crossovers for troubleshooting
            logger.info("\n=== LAST 10 CROSSOVERS (for debugging) ===")
            logger.info(f"{'Date':<12} | {'Type':<4} | {'Status':<10} | {'Reason'}")
            logger.info("-" * 70)
            
            for signal in all_crossovers[-10:]:
                idx = signal['index']
                sig_date = df['date'].iloc[idx]
                sig_date_str = sig_date.date() if hasattr(sig_date, 'date') else sig_date
                sig_type = signal['type']
                sig_price = signal['price']
                sig_ema = signal['ema']
                
                # Check if ignored
                was_ignored = False
                ignore_reason = None
                for ig in trade_analysis['ignored_signals']:
                    ig_date = ig['date'].date() if hasattr(ig['date'], 'date') else ig['date']
                    if ig_date == sig_date_str and ig['type'] == sig_type:
                        was_ignored = True
                        ignore_reason = ig['reason']
                        break
                
                was_saved = signal['index'] in valid_signal_indices
                
                if was_saved:
                    status = "✓ SAVED"
                elif was_ignored:
                    status = "✗ IGNORED"
                else:
                    status = "- SKIPPED"
                
                reason_text = ignore_reason if ignore_reason else (
                    "In completed trade" if was_saved else "Not in completed trade"
                )
                
                logger.info(f"{str(sig_date_str):<12} | {sig_type.upper():<4} | {status:<10} | {reason_text}")
                logger.info(f"             Price: {sig_price:.2f}, EMA: {sig_ema:.2f}")
            
            logger.info("=" * 70)
            
            return {
                'success': True,
                'latest_signal': latest_signal,
                'all_signals': saved_signals,
                'trades': {
                    'completed': len(trade_analysis['trades']),
                    'winning': trade_analysis['winning_trades'],
                    'losing': trade_analysis['losing_trades'],
                    'win_rate': round(trade_analysis['win_rate'], 2),
                    'total_return': round(trade_analysis['total_return'], 2),
                    'avg_return': round(trade_analysis['avg_return'], 2),
                    'avg_days_held': round(trade_analysis['avg_days_held'], 1),
                    'avg_winning_trade': round(trade_analysis['avg_winning_trade'], 2),
                    'avg_losing_trade': round(trade_analysis['avg_losing_trade'], 2)
                },
                'ignored_signals': {
                    'count': trade_analysis['ignored_stats']['count'] if trade_analysis['ignored_stats'] else 0,
                    'buy_ignored': trade_analysis['ignored_stats']['buy_signals_ignored'] if trade_analysis['ignored_stats'] else 0,
                    'sell_ignored': trade_analysis['ignored_stats']['sell_signals_ignored'] if trade_analysis['ignored_stats'] else 0
                },
                'metadata': {
                    'ema_period': ema_period,
                    'min_holding_days': min_holding_days,
                    'total_crossovers_detected': len(all_crossovers),
                    'valid_signals_saved': len(saved_signals),
                    'signals_ignored': len(all_crossovers) - len(saved_signals),
                    'data_points_processed': len(df),
                    'date_range': {
                        'start': df['date'].min().isoformat(),
                        'end': df['date'].max().isoformat()
                    },
                    'generated_at': datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Error generating signals: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e),
                'latest_signal': None,
                'all_signals': [],
                'trades': None
            }
    
    def get_signals_history(self, limit: int = 50) -> List[Dict]:
        """Get historical trading signals"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT signal_date, signal_type, current_price, ema_value, days_since_last_signal
                FROM nepse_trading_signals
                ORDER BY signal_date DESC
                LIMIT ?
            ''', (limit,))
            
            signals = []
            for row in cursor.fetchall():
                signals.append({
                    'date': str(row[0]),
                    'type': row[1],
                    'price': round(row[2], 2),
                    'ema': round(row[3], 2),
                    'days_since_last': row[4]
                })
            
            return signals
            
        except Exception as e:
            logger.error(f"Error fetching signal history: {e}")
            return []
        finally:
            conn.close()
    
    def get_trades_history(self, include_skipped: bool = False, limit: int = 50) -> List[Dict]:
        """
        Get historical completed trades
        
        Args:
            include_skipped: Currently unused - all trades in DB are completed trades
            limit: Maximum number of trades to return
        
        Returns:
            List of trade dictionaries
        """
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT entry_date, entry_price, exit_date, exit_price, 
                    return_pct, days_held, result
                FROM nepse_completed_trades
                ORDER BY entry_date DESC
                LIMIT ?
            ''', (limit,))
            
            trades = []
            for row in cursor.fetchall():
                trades.append({
                    'entry_date': str(row[0]),
                    'entry_price': round(row[1], 2),
                    'exit_date': str(row[2]),
                    'exit_price': round(row[3], 2),
                    'return': round(row[4], 2),
                    'days_held': row[5],
                    'result': row[6],
                    'was_skipped': False  # All trades in DB are completed, not skipped
                })
            
            return trades
            
        except Exception as e:
            logger.error(f"Error fetching trades history: {e}")
            return []
        finally:
            conn.close()
    
    def get_signal_statistics(self) -> Dict:
        """Get overall signal and trade statistics"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            # Signal stats
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN signal_type = 'buy' THEN 1 ELSE 0 END) as buys,
                    SUM(CASE WHEN signal_type = 'sell' THEN 1 ELSE 0 END) as sells
                FROM nepse_trading_signals
            ''')
            signal_stats = cursor.fetchone()
            
            # Trade stats
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END) as losses,
                    AVG(return_pct) as avg_return,
                    AVG(days_held) as avg_days,
                    SUM(return_pct) as total_return
                FROM nepse_completed_trades
            ''')
            trade_stats = cursor.fetchone()
            
            total_trades = trade_stats[0] or 0
            wins = trade_stats[1] or 0
            
            return {
                'signals': {
                    'total': signal_stats[0] or 0,
                    'buy': signal_stats[1] or 0,
                    'sell': signal_stats[2] or 0
                },
                'trades': {
                    'completed': total_trades,
                    'wins': wins,
                    'losses': trade_stats[2] or 0,
                    'win_rate': round((wins / total_trades * 100) if total_trades > 0 else 0, 2),
                    'avg_return': round(trade_stats[3] or 0, 2),
                    'avg_days_held': round(trade_stats[4] or 0, 1),
                    'total_return': round(trade_stats[5] or 0, 2)
                }
            }
            
        except Exception as e:
            logger.error(f"Error calculating statistics: {e}")
            return {}
        finally:
            conn.close()