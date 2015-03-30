"""
@file           parser.py
@description    BATS Chi-X Europe PITCH Specification v4.4.1 parser core
@author         Andrian Yablonskyy (andrian.yablonskyy@gmail.com)
@date           23 Mar 2015

This file is released under MIT license.
More detailed information is stored in LICENSE.txt
"""

import traceback
from datetime import datetime, date
from time import mktime
from struct import *


class MsgBody(object):
    def __init__(self, data):
        self.data = data

    def get_fields(self, names):
        start = 0
        fields = {}
        for arg in names:
            fields[arg[0]] = self.data[start: start + arg[1]]
            # print("Name: ", arg[0], arg[1], fields[arg[0]])
            start += arg[1]

        return fields

    def __str__(self):
        return self.data


class Exchange():

    def flag(func):
        def wrapper(_flag, *args, **kwargs):
            try:
                return func(chr(_flag), *args, **kwargs)
            except Exception as ex:
                print("Error! In ", func.__name__, str(ex))

        return wrapper

    def process_msg_header(msg_len):
        def wrap(func):
            def wrapper(self, ts, data, *args, **kwargs):
                try:
                    # print("Call %s(%s)" %
                    #     (func.__name__, data[0:msg_len])
                    # )
                    msg_data = MsgBody(data[0:msg_len])
                    fields = func(self, msg_data, *args, **kwargs)
                    fields['receive_timestamp'] = ts
                    q_map = self.map_quote(fields)
                    self.write_quote(*q_map)

                except Exception as ex:
                    print("Error! Message", func.__name__,
                          self.to_bstr(data[0:msg_len]),
                          "ignored. (%s)" % str(ex))
                    traceback.print_exc()
                return data[msg_len:]

            return wrapper

        return wrap

    @staticmethod
    @flag
    def market_mechanism(key):
        return {
            "1": 1,
            "2": 2,
            "3": 3,
            "4": 4,
        }.get(key)

    @staticmethod
    @flag
    def trading_mode(key):
        return {
            "1": 1,
            "2": 2,
            "3": 3,
            "4": 4,
            "5": 5,
            "6": 6,
            "7": 7,
            "O": 8,
            "K": 9,
            "I": 10,
            "U": 11
        }.get(key)

    @staticmethod
    @flag
    def transaction_category(key):
        return {
            "P": 1,
            "D": 2,
            "T": 3,
            "G": 4,
            "F": 5
        }.get(key)

    @staticmethod
    @flag
    def negotiated_trade(key):
        return {
            "N": 1,
        }.get(key, 2)

    @staticmethod
    @flag
    def crossing_trade(key):
        return {
            "X": 1,
        }.get(key, 2)

    @staticmethod
    @flag
    def modification_indicator(key):
        return {
            "A": 1,
            "C": 2,
        }.get(key, 3)

    @staticmethod
    @flag
    def benchmark_indicator(key):
        return {
            "B": 1,
        }.get(key, 2)

    @staticmethod
    @flag
    def ex_cum_dividend(key):
        return {
            "E": 1,
        }.get(key, 2)

    @staticmethod
    @flag
    def offbook_automated_indicator(key):
        return {
            "Q": 1,
            "M": 2,
        }.get(key, 3)

    @staticmethod
    @flag
    def publication_indicator(key):
        return {
            "1": 1,
        }.get(key, 2)

    def parse_order_execution_flag(self, data):
        return {
            'market_mechanism': self.market_mechanism(data[0]),
            'trading_mode': self.trading_mode(data[1]),
            'excum_dividen': self.ex_cum_dividend(data[2])
        }

    def parse_trade_flags(self, data):
        return {
            'market_mechanism': self.market_mechanism(data[0]),
            'trading_mode': self.trading_mode(data[1]),
            'transaction_category': self.transaction_category(data[2]),
            'excum_dividen': self.ex_cum_dividend(data[3])
        }

    def parse_trade_report_flags(self, data):
        return {
            'trade_timing_indicator': {
                "1": 1,
                "2": 2,
            }.get(data[0], 3),
            'market_mechanism': self.market_mechanism(data[0]),
            'trading_mode': self.trading_mode(data[1]),
            'transaction_category': self.transaction_category(data[2]),
            'negotiated_trade': self.negotiated_trade(data[3]),
            'crossing_trade': self.crossing_trade(data[4]),
            'modification_indicator': self.modification_indicator(data[5]),
            'benchmark_indicator': self.benchmark_indicator(data[6]),
            'excum_dividen': self.ex_cum_dividend(data[7]),
            'publication_indicator': self.publication_indicator(data[8]),
            'offbook_automated_indicator':
                self.offbook_automated_indicator(data[9]),
        }

    # Clear msg parser
    @process_msg_header(8)
    def msg_clear(self, data):
        names = (
            ('pitch_symbol', 8),
        )
        return data.get_fields(names)

    # Add Order Message
    @process_msg_header(45)
    def msg_add_order(self, data):
        names = (
            ('pitch_order', 12),
            ('pitch_side', 1),
            ('pitch_shares_s', 6),
            ('pitch_symbol', 6),
            ('pitch_price_s', 10),
            ('pitch_display', 1)
        )
        return data.get_fields(names)

    # Add Order Message — Long Form
    @process_msg_header(60)
    def msg_add_order_long(self, data):
        names = (
            ('pitch_order', 12),
            ('pitch_side', 1),
            ('pitch_shares_l', 10),
            ('pitch_symbol', 8),
            ('pitch_price_l', 19),
            ('pitch_display', 1)
        )
        return data.get_fields(names)

    # Add Order Message — Expanded Form
    @process_msg_header(64)
    def msg_add_order_exp(self, data):
        names = (
            ('pitch_order', 12),
            ('pitch_side', 1),
            ('pitch_shares_l', 10),
            ('pitch_symbol', 8),
            ('pitch_price_l', 19),
            ('pitch_type', 1),
            ('pitch_participant', 4)
        )
        return data.get_fields(names)

    # Executed Order Message
    @process_msg_header(42)
    def msg_order_executed(self, data):
        names = (
            ('pitch_order', 12),
            ('pitch_shares_s', 6),
            ('pitch_execution', 12),
            ('flags', 3)
        )
        fields = data.get_fields(names)

        fields.update(self.parse_order_execution_flag(fields['flags']))
        del fields['flags']
        return fields

    # Executed Order Message — Long Form
    @process_msg_header(46)
    def msg_order_executed_long(self, data):
        names = (
            ('pitch_order', 12),
            ('pitch_shares_l', 10),
            ('pitch_execution', 12),
            ('flags', 3)
        )
        fields = data.get_fields(names)

        fields.update(self.parse_order_execution_flag(fields['flags']))
        del fields['flags']
        return fields

    # Cancel Order Message
    @process_msg_header(27)
    def msg_order_cancel(self, data):
        names = (
            ('pitch_order', 12),
            ('pitch_shares_s', 6)
        )
        return data.get_fields(names)

    # Cancel Order Message — Long Form
    @process_msg_header(31)
    def msg_order_cancel_long(self, data):
        names = (
            ('pitch_order', 12),
            ('pitch_shares_l', 10)
        )
        return data.get_fields(names)

    # Trade Message
    @process_msg_header(60)
    def msg_trade(self, data):
        names = (
            ('pitch_order', 12),
            ('pitch_side', 1),
            ('pitch_shares_s', 6),
            ('pitch_symbol', 6),
            ('pitch_price_s', 10),
            ('pitch_execution', 12),
            ('flags', 4),
        )
        fields = data.get_fields(names)

        fields.update(self.parse_trade_flags(fields['flags']))
        del fields['flags']

        return fields

    # Trade Message - Long form
    @process_msg_header(75)
    def msg_trade_long(self, data):
        names = (
            ('pitch_order', 12),
            ('pitch_side', 1),
            ('pitch_shares_s', 10),
            ('pitch_symbol', 8),
            ('pitch_price_l', 19),
            ('pitch_execution', 12),
            ('flags', 4)
        )
        fields = data.get_fields(names)

        fields.update(self.parse_trade_flags(fields['flags']))
        del fields['flags']

        return fields

    # Trade Break Message
    @process_msg_header(21)
    def msg_trade_break(self, data):
        names = (
            ('pitch_execution', 12),
        )
        return data.get_fields(names)

    # Trade Report Message
    @process_msg_header(94)
    def msg_trade_report(self, data):
        names = (
            ('pitch_shares_l', 12),
            ('pitch_symbol', 8),
            ('pitch_price_l', 19),
            ('pitch_trade', 12),
            ('date', 8),
            ('time', 8),
            ('pitch_exec_venue', 4),
            ('pitch_currency', 3),
            ('flags', 11)
        )
        fields = data.get_fields(names)

        midnight = mktime(
            datetime.strptime(fields['date'], "%Y%m%d").timetuple()
        )
        fields['tradetime'] = self.date_format(midnight + int(fields['time']))

        del fields['date']
        del fields['time']

        fields.update(self.parse_trade_report_flags(fields['flags']))
        del fields['flags']

        return fields

    # Trading Status Message
    @process_msg_header(21)
    def msg_trading_status(self, data):
        names = (
            ('pitch_symbol', 8),
            ('pitch_status', 1),
            ('pitch_reserved', 3)
        )
        fields = data.get_fields(names)
        fields['pitch_trading_status'] = \
            {'T': 1, 'R': 2, 'C': 3, 'S': 4, 'N': 5, 'V': 6, 'O': 7,
             'E': 8, 'H': 9, 'M': 10, 'P': 11} \
            .get(fields['pitch_trading_status'])
        return fields

    # Statistics Message
    @process_msg_header(38)
    def msg_statistics(self, data):
        names = (
            ('pitch_symbol', 8),
            ('pitch_price_l', 19),
            ('pitch_statistic_type', 1),
            ('pitch_price_determination', 1)
        )
        fields = data.get_fields(names)
        fields['pitch_statistic_type'] = \
            {'C': 1, 'H': 2, 'L': 3, 'O': 4, 'P': 5} \
            .get(fields['pitch_statistic_type'])
        fields['pitch_price_determination'] = {'0': 1, '1': 2} \
            .get(fields['pitch_price_determination'])
        return fields

    # Auction Update Message
    @process_msg_header(76)
    def msg_auction_update(self, data):
        names = (
            ('pitch_symbol', 8),
            ('pitch_auction_type', 1),
            ('pitch_reference_price_l', 19),
            ('pitch_buy_shares_l', 10),
            ('pitch_sell_shares_l', 10),
            ('pitch_indicative_price_l', 19)
        )
        fields = data.get_fields(names)
        fields['pitch_auction_type'] = \
            {'O': 1, 'C': 2, 'H': 3, 'V': 4} \
            .get(fields['pitch_auction_type'])
        return fields

    # Auction Summary Message
    @process_msg_header(47)
    def msg_auction_summary(self, data):
        names = (
            ('pitch_symbol', 8),
            ('pitch_auction_type', 1),
            ('pitch_price_l', 19),
            ('pitch_share_l', 10)
        )
        fields = data.get_fields(names)
        fields['pitch_auction_type'] = \
            {'O': 1, 'C': 2, 'H': 3, 'V': 4} \
            .get(fields['pitch_auction_type'])
        return fields

    def __init__(self, name, **params):

        self.types = {
            "s": self.msg_clear,
            "A": self.msg_add_order,
            "c": self.msg_add_order_long,
            "t": self.msg_add_order_exp,
            "E": self.msg_order_executed,
            "e": self.msg_order_executed_long,
            "X": self.msg_order_cancel,
            "x": self.msg_order_cancel_long,
            "P": self.msg_trade,
            "q": self.msg_trade_long,
            "B": self.msg_trade_break,
            "O": self.msg_trade_report,
            "H": self.msg_trading_status,
            "Z": self.msg_statistics,
            "k": self.msg_auction_update,
            "j": self.msg_auction_summary
        }

        self.date = date.today()

    """
    Parse data entry point
    @param      bytes_data, RAW data to parse
    @param      date, timestamp
    """
    def parse(self, bytes_data):
        self.midnight = mktime(datetime.today().timetuple())

        data = bytes_data.decode('utf-8')
        for msg in data.split('\n'):
            if not msg:
                continue

            msg = msg[1:]
            # Check minimum message length (timestamp + type)
            ts = int(msg[0:7])
            m_type = msg[8]

            # ignore unknown messages
            if m_type in self.types.keys():
                ts = self.date_format(self.midnight + ts)
                self.types[m_type](ts, msg[9:])

        # closing quotes, also flush rows...
        self.close_quotes()

    def map_quote(self, fields):

        """
        Type: Description
        Alpha:  A string of ASCII letters (A–Z), left justified and space
        padded on the right.
        Alphanumeric: A string of ASCII numbers and letters (A–Z, 0–9), left
        justified and space padded on the right.
        Base 36 Numeric: A string of ASCII numbers and letters (A–Z, 0–9),
        representing base 36 digits, right justified and zero filled on the
        left. Typically used for Order IDs and Execution IDs.
        Numeric: A string of ASCII numbers (0–9), right justified and zero
        filled on the left.
        Price: A string of ASCII numbers (0–9) consisting of six whole digits
        followed by four decimal digits. The whole number portion is zero
        filled on the left; the decimal portion is zero filled on the right.
        The decimal point is implied by position and does not explicitly appear
        in the field.
        Long Price: As with Prices above, but this field consists of 12 whole
        number digits followed by seven decimal digits.
        Timestamp: A string of ASCII numbers (0–9) representing the whole
        number of milliseconds past midnight London time, right justified and
        zero padded on the left, with no decimal point.

        field name          | field data type   | description
        pitch_order         |                   | Obfuscated Order ID or Order
                                                  ID of the invisible order or
                                                  negotiated trade.
        pitch_trade         | Base 36 Numeric   | BATS Chi-X Europe generated
                                                  identifier of this trade.
                                                  This identifier is guaranteed
                                                  to be unique for at least 7
                                                  calendar days.
        pitch_shares        | Numeric           | Number of shares executed.
        pitch_side          | Alpha             | B = Buy or S = Sell
        pitch_symbol        | Alphanumeric      | Symbol, right padded with
                                                  spaces.
        pitch_price         | Price             | Order price
        pitch_priceL        | Long Price        | The quote price
        pitch_display       | "Y"               | Always "Y"
        pitch_type          | Alphanumeric      | Attributed order type
                                                  indicator. Currently defined
                                                  values: S - ‘SI’ Quote
        pitch_participant   | Alphanumeric      | Attributes this quote to a
                                                  particular participant.
        pitch_receive_timestamp   | datetime    | Datetime of receive RAW data
        pitch_price_determination | Alphanumeric| “0” = Normal
                                                  “1” = Manual (Price override
                                                  by Market Supervision)
        pitch_statistic_type| Alpha             | “C” = Closing Price /-
                                                  “H” = High Price /-
                                                  “L” = Low Price /-
                                                  “O” = Opening Price /-
                                                  “P” = Previous Closing
                                                        Price / -
        pitch_reserved      | Alpha             |
        pitch_status        | Alpha             | “T” = Trading /-
                                                  “R” = Off-Book Reporting / -
                                                  “C” = Closed / -
                                                  “S” = Suspended /-
                                                  “N” = No Reference Price /-
                                                  “V” = Volatility
                                                        Interruption /-
                                                  “O” = Opening Auction 2 /-
                                                  “E” = Closing Auction 2 /-
                                                  “H” = Halt 2 /-
                                                  “M” = Market Order
                                                        Imbalance 2 /-
                                                  “P” = Price Monitoring
                                                        Extension 2 /-
        pitch_currency      | Alphanumeric      | Traded currency.
        pitch_tradetime     | datetime          | Datetime of trade
        pitch_execution     | Base 36 Numeric   | BATS execution identifier of
                                                  the execution that was
                                                  broken. Refers to a
                                                  previously sent Order
                                                  Execution Message or Trade
                                                  Message.
        pitch_exec_venue    | Alphanumeric      | The venue on which the trade
                                                  executed, when applicable.
                                                  This will contain the MIC
                                                  representing the venue on
                                                  which the trade occurred,
                                                  where applicable. e.g. for
                                                  BATS Chi-X Europe NT trades,
                                                  this shall be BATE or CHIX
                                                  as applicable. Where no MIC
                                                  is applicable, this field
                                                  will be blank.
                                                  A special value of AUT can be
                                                  used to identify OTC trades
                                                  executed in an automated
                                                  manner.
        pitch_indicative_price | Long Price        | Price at which the auction
                                                  would match if executed at
                                                  the time of the message
        pitch_reference_price  | Long Price        |
        """

        # TODO: Map fields described above to quote

        return "", 0, "", ""

    def date_format(self, stamp):
        try:
            return datetime.fromtimestamp(stamp)
        except (IndexError, ValueError):
            # Log or raise exception
            return None
