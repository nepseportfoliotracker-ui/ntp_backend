# ema_signal_service.py - 4-Day EMA Buy/Sell Signal Generator with 2-Day Holding Period

import logging
import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class EMASignalService:
    """
    Generate buy/sell signals based on 4-day EMA crossovers with 2-day minimum holding period.
    
    Signal Rules:
    1. BUY: When price crosses ABOVE the 4-day EMA
    2. SELL: When price crosses BELOW the 4-day EMA
    3. Minimum holding period: 2 days after any signal before allowing opposite signal
    """
    
    def __init__(self, db_service, nepse_history_service, ema_period=4, min_holding_days=2):
        self.db_service = db_service
        self.nepse_history_service = nepse_history_service
        self.ema_period = ema_period
        self.min_holding_days = min_holding_days
        self._init_signals_table()
        logger.info(f"EMA Signal Service initialized (EMA: {ema_period} days, Min holding: {min_holding_days} days)")
    
    def _init_signals_table(self):
        """Initialize table to store trading signals"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ema_trading_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_date DATE NOT NULL UNIQUE,
                    signal_type TEXT NOT NULL,
                    index_price REAL NOT NULL,
                    ema_value REAL NOT NULL,
                    can_trade INTEGER DEFAULT 1,
                    holding_period_active INTEGER DEFAULT 0,
                    holding_days_remaining INTEGER DEFAULT 0,
                    last_signal_date DATE,
                    days_since_last_signal INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    CHECK (signal_type IN ('BUY', 'SELL', 'HOLD'))
                )
            ''')
            
            # Trade execution history
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ema_trade_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry_date DATE NOT NULL,
                    entry_price REAL NOT NULL,
                    entry_signal TEXT NOT NULL,
                    exit_date DATE,
                    exit_price REAL,
                    exit_signal TEXT,
                    holding_days INTEGER,
                    profit_loss REAL,
                    profit_loss_percent REAL,
                    trade_status TEXT DEFAULT 'OPEN',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    closed_at DATETIME,
                    CHECK (entry_signal IN ('BUY', 'SELL')),
                    CHECK (exit_signal IN ('BUY', 'SELL', NULL)),
                    CHECK (trade_status IN ('OPEN', 'CLOSED'))
                )
            ''')
            
            # Signal statistics
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ema_signal_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stat_date DATE NOT NULL UNIQUE,
                    total_signals INTEGER DEFAULT 0,
                    buy_signals INTEGER DEFAULT 0,
                    sell_signals INTEGER DEFAULT 0,
                    hold_signals INTEGER DEFAULT 0,
                    total_trades INTEGER DEFAULT 0,
                    winning_trades INTEGER DEFAULT 0,
                    losing_trades INTEGER DEFAULT 0,
                    win_rate REAL DEFAULT 0,
                    avg_profit_loss REAL DEFAULT 0,
                    total_return REAL DEFAULT 0,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_signal_date ON ema_trading_signals(signal_date DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_trade_status ON ema_trade_history(trade_status)')
            
            conn.commit()
            logger.info("EMA signal tables initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize EMA signal tables: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def calculate_ema(self, prices, period):
        """
        Calculate Exponential Moving Average
        
        Parameters:
        - prices: List or Series of prices
        - period: EMA period (e.g., 4 for 4-day EMA)
        
        Returns:
        - pandas Series with EMA values
        """
        df = pd.DataFrame({'price': prices})
        ema = df['price'].ewm(span=period, adjust=False).mean()
        return ema
    
    def detect_crossover(self, current_price, current_ema, prev_price, prev_ema):
        """
        Detect if price crossed above or below EMA
        
        Returns:
        - 'BUY': Price crossed above EMA
        - 'SELL': Price crossed below EMA
        - 'HOLD': No crossover detected
        """
        # Price crossed above EMA (bullish crossover)
        if prev_price <= prev_ema and current_price > current_ema:
            return 'BUY'
        
        # Price crossed below EMA (bearish crossover)
        if prev_price >= prev_ema and current_price < current_ema:
            return 'SELL'
        
        # No crossover
        return 'HOLD'
    
    def generate_signals(self, force=False):
        """
        Generate trading signals for all available historical data
        
        Parameters:
        - force: If True, regenerate all signals. If False, only generate new signals.
        
        Returns:
        - Dictionary with signal generation results
        """
        try:
            logger.info(f"Generating EMA trading signals (EMA period: {self.ema_period}, Min holding: {self.min_holding_days} days)")
            
            # Get yearly data (365 days) to ensure enough data for EMA calculation
            history_data = self.nepse_history_service.get_yearly_data()
            
            if not history_data:
                logger.warning("No NEPSE history data available for signal generation")
                return {
                    'success': False,
                    'error': 'No historical data available',
                    'signals_generated': 0
                }
            
            # Convert to DataFrame
            df = pd.DataFrame(history_data)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date', ascending=True).reset_index(drop=True)
            
            # Calculate EMA
            df['ema'] = self.calculate_ema(df['index_value'], self.ema_period)
            
            # Generate signals
            signals = []
            last_signal = None
            last_signal_date = None
            
            for i in range(1, len(df)):
                current_date = df.loc[i, 'date']
                current_price = df.loc[i, 'index_value']
                current_ema = df.loc[i, 'ema']
                
                prev_price = df.loc[i-1, 'index_value']
                prev_ema = df.loc[i-1, 'ema']
                
                # Detect crossover
                crossover_signal = self.detect_crossover(
                    current_price, current_ema,
                    prev_price, prev_ema
                )
                
                # Check if we can trade (respect holding period)
                can_trade = True
                holding_period_active = False
                holding_days_remaining = 0
                days_since_last_signal = None
                
                if last_signal_date:
                    days_since_last_signal = (current_date - last_signal_date).days
                    
                    # If we're within holding period, cannot take opposite signal
                    if days_since_last_signal < self.min_holding_days:
                        holding_period_active = True
                        holding_days_remaining = self.min_holding_days - days_since_last_signal
                        
                        # If signal is opposite to last signal, cannot trade
                        if crossover_signal != 'HOLD' and crossover_signal != last_signal:
                            can_trade = False
                            crossover_signal = 'HOLD'  # Force HOLD during holding period
                
                # Record signal if it's a BUY/SELL or if we want to track HOLD periods
                if crossover_signal in ['BUY', 'SELL']:
                    signals.append({
                        'date': current_date.date(),
                        'signal_type': crossover_signal,
                        'price': current_price,
                        'ema': current_ema,
                        'can_trade': can_trade,
                        'holding_period_active': holding_period_active,
                        'holding_days_remaining': holding_days_remaining,
                        'last_signal_date': last_signal_date.date() if last_signal_date else None,
                        'days_since_last_signal': days_since_last_signal
                    })
                    
                    # Update last signal tracking
                    if can_trade:
                        last_signal = crossover_signal
                        last_signal_date = current_date
            
            # Save signals to database
            saved_count = self._save_signals(signals)
            
            # Calculate and save statistics
            self._calculate_statistics()
            
            # Get latest signal
            latest_signal = self.get_latest_signal()
            
            # Get trade summary
            trade_summary = self.get_trade_summary()
            
            logger.info(f"Signal generation completed: {saved_count} signals generated")
            
            return {
                'success': True,
                'signals_generated': saved_count,
                'latest_signal': latest_signal,
                'trade_summary': trade_summary,
                'parameters': {
                    'ema_period': self.ema_period,
                    'min_holding_days': self.min_holding_days
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to generate signals: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e),
                'signals_generated': 0
            }
    
    def _save_signals(self, signals):
        """Save generated signals to database"""
        if not signals:
            return 0
        
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            saved_count = 0
            
            for signal in signals:
                cursor.execute('''
                    INSERT OR REPLACE INTO ema_trading_signals
                    (signal_date, signal_type, index_price, ema_value, can_trade, 
                     holding_period_active, holding_days_remaining, last_signal_date, days_since_last_signal)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    signal['date'],
                    signal['signal_type'],
                    signal['price'],
                    signal['ema'],
                    int(signal['can_trade']),
                    int(signal['holding_period_active']),
                    signal['holding_days_remaining'],
                    signal['last_signal_date'],
                    signal['days_since_last_signal']
                ))
                saved_count += 1
            
            conn.commit()
            logger.info(f"Saved {saved_count} signals to database")
            return saved_count
            
        except Exception as e:
            logger.error(f"Failed to save signals: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    def _calculate_statistics(self):
        """Calculate and save signal statistics"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get all tradeable signals
            cursor.execute('''
                SELECT signal_type, COUNT(*) as count
                FROM ema_trading_signals
                WHERE can_trade = 1
                GROUP BY signal_type
            ''')
            
            signal_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            total_signals = sum(signal_counts.values())
            buy_signals = signal_counts.get('BUY', 0)
            sell_signals = signal_counts.get('SELL', 0)
            hold_signals = signal_counts.get('HOLD', 0)
            
            # Simulate trades to calculate performance
            trade_stats = self._simulate_trades()
            
            # Save statistics
            cursor.execute('''
                INSERT OR REPLACE INTO ema_signal_stats
                (stat_date, total_signals, buy_signals, sell_signals, hold_signals,
                 total_trades, winning_trades, losing_trades, win_rate, avg_profit_loss, total_return, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                datetime.now().date(),
                total_signals,
                buy_signals,
                sell_signals,
                hold_signals,
                trade_stats['total_trades'],
                trade_stats['winning_trades'],
                trade_stats['losing_trades'],
                trade_stats['win_rate'],
                trade_stats['avg_profit_loss'],
                trade_stats['total_return']
            ))
            
            conn.commit()
            logger.info(f"Statistics calculated: {total_signals} total signals, {trade_stats['total_trades']} trades")
            
        except Exception as e:
            logger.error(f"Failed to calculate statistics: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def _simulate_trades(self):
        """Simulate trades based on signals to calculate performance"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get all tradeable signals in chronological order
            cursor.execute('''
                SELECT signal_date, signal_type, index_price
                FROM ema_trading_signals
                WHERE can_trade = 1
                ORDER BY signal_date ASC
            ''')
            
            signals = cursor.fetchall()
            
            trades = []
            current_position = None  # None, 'LONG'
            entry_date = None
            entry_price = None
            entry_signal = None
            
            for signal_date, signal_type, price in signals:
                # Entry: BUY signal and no position
                if signal_type == 'BUY' and current_position is None:
                    current_position = 'LONG'
                    entry_date = signal_date
                    entry_price = price
                    entry_signal = 'BUY'
                
                # Exit: SELL signal and have LONG position
                elif signal_type == 'SELL' and current_position == 'LONG':
                    holding_days = (datetime.strptime(str(signal_date), '%Y-%m-%d') - 
                                  datetime.strptime(str(entry_date), '%Y-%m-%d')).days
                    
                    profit_loss = price - entry_price
                    profit_loss_percent = (profit_loss / entry_price) * 100
                    
                    trades.append({
                        'entry_date': entry_date,
                        'entry_price': entry_price,
                        'entry_signal': entry_signal,
                        'exit_date': signal_date,
                        'exit_price': price,
                        'exit_signal': 'SELL',
                        'holding_days': holding_days,
                        'profit_loss': profit_loss,
                        'profit_loss_percent': profit_loss_percent
                    })
                    
                    current_position = None
            
            # Calculate statistics
            total_trades = len(trades)
            winning_trades = sum(1 for t in trades if t['profit_loss'] > 0)
            losing_trades = sum(1 for t in trades if t['profit_loss'] < 0)
            
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
            avg_profit_loss = sum(t['profit_loss_percent'] for t in trades) / total_trades if total_trades > 0 else 0
            total_return = sum(t['profit_loss_percent'] for t in trades)
            
            return {
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': round(win_rate, 2),
                'avg_profit_loss': round(avg_profit_loss, 2),
                'total_return': round(total_return, 2),
                'trades': trades
            }
            
        except Exception as e:
            logger.error(f"Failed to simulate trades: {e}")
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate': 0,
                'avg_profit_loss': 0,
                'total_return': 0,
                'trades': []
            }
        finally:
            conn.close()
    
    def get_latest_signal(self):
        """Get the most recent trading signal"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT signal_date, signal_type, index_price, ema_value, can_trade,
                       holding_period_active, holding_days_remaining, days_since_last_signal
                FROM ema_trading_signals
                ORDER BY signal_date DESC
                LIMIT 1
            ''')
            
            row = cursor.fetchone()
            
            if row:
                return {
                    'date': str(row[0]),
                    'signal': row[1],
                    'price': row[2],
                    'ema': row[3],
                    'can_trade': bool(row[4]),
                    'holding_period_active': bool(row[5]),
                    'holding_days_remaining': row[6],
                    'days_since_last_signal': row[7]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get latest signal: {e}")
            return None
        finally:
            conn.close()
    
    def get_trade_summary(self):
        """Get summary of trade performance"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT total_signals, buy_signals, sell_signals, hold_signals,
                       total_trades, winning_trades, losing_trades, win_rate,
                       avg_profit_loss, total_return
                FROM ema_signal_stats
                ORDER BY stat_date DESC
                LIMIT 1
            ''')
            
            row = cursor.fetchone()
            
            if row:
                return {
                    'total_signals': row[0],
                    'buy_signals': row[1],
                    'sell_signals': row[2],
                    'hold_signals': row[3],
                    'total_trades': row[4],
                    'winning_trades': row[5],
                    'losing_trades': row[6],
                    'win_rate': row[7],
                    'avg_profit_loss': row[8],
                    'total_return': row[9]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get trade summary: {e}")
            return None
        finally:
            conn.close()
    
    def get_all_signals(self, limit=100):
        """Get all trading signals"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT signal_date, signal_type, index_price, ema_value, can_trade,
                       holding_period_active, holding_days_remaining, days_since_last_signal
                FROM ema_trading_signals
                ORDER BY signal_date DESC
                LIMIT ?
            ''', (limit,))
            
            signals = []
            for row in cursor.fetchall():
                signals.append({
                    'date': str(row[0]),
                    'signal': row[1],
                    'price': row[2],
                    'ema': row[3],
                    'can_trade': bool(row[4]),
                    'holding_period_active': bool(row[5]),
                    'holding_days_remaining': row[6],
                    'days_since_last_signal': row[7]
                })
            
            return signals
            
        except Exception as e:
            logger.error(f"Failed to get signals: {e}")
            return []
        finally:
            conn.close()
    
    def get_signal_for_date(self, target_date):
        """Get signal for a specific date"""
        conn = self.db_service.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT signal_date, signal_type, index_price, ema_value, can_trade,
                       holding_period_active, holding_days_remaining, days_since_last_signal
                FROM ema_trading_signals
                WHERE signal_date = ?
            ''', (target_date,))
            
            row = cursor.fetchone()
            
            if row:
                return {
                    'date': str(row[0]),
                    'signal': row[1],
                    'price': row[2],
                    'ema': row[3],
                    'can_trade': bool(row[4]),
                    'holding_period_active': bool(row[5]),
                    'holding_days_remaining': row[6],
                    'days_since_last_signal': row[7]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get signal for date: {e}")
            return None
        finally:
            conn.close()