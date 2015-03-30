"""
@file           parser.py
@description    BATS Chi-X Europe PITCH Specification v4.4.1 parser core
@author         Andrian Yablonskyy (andrian.yablonskyy@gmail.com)
@date           23 Mar 2015

This file is released under MIT license.
More detailed information is stored in LICENSE.txt
"""

import exchanges
import traceback
from datetime import datetime, date
from time import mktime
from struct import unpack as unpack, error as unpack_error
import ctypes

"""
Issues:
1. Too many files opened. Variable contract is equal to message sequence #.
In this case we have got an error "Too many files opened" because Exchange
open a file for contract and leave it opened until parse a whole stream.

"""


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


class Flags(ctypes.Union):
    class FlagsBits(ctypes.LittleEndianStructure):
        _fields_ = [
            ("market_mechanism", ctypes.c_uint8, 3),
            ("trading_mode", ctypes.c_uint8, 4),
            ("transaction_category", ctypes.c_uint8, 3),
            ("modification_indicator", ctypes.c_uint8, 3),
            ("offbook_automated_indicator", ctypes.c_uint8, 3),
            ("trade_timing_indicator", ctypes.c_uint8, 3),
            ("benchmark_indicator", ctypes.c_uint8, 3),
            ("crossing_trade", ctypes.c_uint8, 3),
            ("ex_cum_dividend", ctypes.c_uint8, 3),
            ("negotiated_trade", ctypes.c_uint8, 3),
            ("publication_indicator", ctypes.c_uint8, 3),
            ("trading_status", ctypes.c_uint8, 3),
            ("statistics_type", ctypes.c_uint8, 3),
            ("login_status", ctypes.c_uint8, 3),
            ("gap_status", ctypes.c_uint8, 3)
        ]

    _fields_ = [
        ("b",      FlagsBits),
        ("asByte", ctypes.c_uint64)
    ]
    _anonymous_ = ("b",)


class Exchange():

    @staticmethod
    def to_bstr(data):
        return " ".join(["%02x" % b for b in data])

    def flag(func):
        def wrapper(self, _flag, *args, **kwargs):
            try:
                return func(self, chr(_flag), *args, **kwargs)
            except Exception as ex:
                print("Error! In ", func.__name__, str(ex))
        return wrapper

    def process_msg_header(msg_len):
        def wrap(func):
            def wrapper(self, data, *args, **kwargs):
                try:
                    self.flags = Flags()
                    # print("Call %s(%s)" %
                    #      (func.__name__, self.to_bstr(data[0:msg_len]))
                    #     )
                    msg_data = MsgBody(data[0:msg_len])
                    fields = func(self, msg_data, *args, **kwargs)
                    q_map = self.map_quote(fields)
                    self.write_quote(*q_map)
                    del self.flags
                except Exception as ex:
                    print("Error! Message", func.__name__,
                          self.to_bstr(data[0:msg_len]),
                          "ignored. (%s)" % str(ex))
                    traceback.print_exc()
                return data[msg_len:]

            return wrapper

        return wrap

    @flag
    def market_mechanism(self, key):
        self.flags.market_mechanism = {
            "1": 1,
            "2": 2,
            "3": 3,
            "4": 4,
        }.get(key, 0)

    @flag
    def trading_mode(self, key):
        self.flags.trading_mode = {
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
        }.get(key, 0)

    @flag
    def transaction_category(self, key):
        self.flags.transaction_category = {
            "P": 1,
            "D": 2,
            "T": 3,
            "G": 4,
            "F": 5
        }.get(key, 0)

    @flag
    def negotiated_trade(self, key):
        self.flags.negotiated_trade = {
            "N": 1,
        }.get(key, 0)

    @flag
    def crossing_trade(self, key):
        self.flags.crossing_trade = {
            "X": 1,
        }.get(key, 0)

    @flag
    def modification_indicator(self, key):
        self.flags.modification_indicator = {
            "A": 1,
            "C": 2,
        }.get(key, 0)

    @flag
    def benchmark_indicator(self, key):
        self.flags.benchmark_indicator = {
            "B": 1,
        }.get(key, 0)

    @flag
    def ex_cum_dividend(self, key):
        self.flags.ex_cum_dividend = {
            "E": 1,
        }.get(key, 0)

    @flag
    def offbook_automated_indicator(self, key):
        self.flags.offbook_automated_indicator = {
            "Q": 1,
            "M": 2,
        }.get(key, 0)

    @flag
    def publication_indicator(self, key):
        self.flags.publication_indicator = {
            "1": 1,
        }.get(key, 0)

    @flag
    def trade_timing_indicator(self, key):
        self.flags.trade_timing_indicator = {
            "1": 1,
            "2": 2,
        }.get(key, 0)

    def parse_order_execution_flag(self, data):
        self.market_mechanism(data[0])
        self.trading_mode(data[1])
        self.ex_cum_dividend(data[2])

    def parse_trade_flags(self, data):
        self.market_mechanism(data[0])
        self.trading_mode(data[1])
        self.transaction_category(data[2])
        self.ex_cum_dividend(data[3])

    def parse_trade_report_flags(self, data):
        self.trade_timing_indicator(data[0])
        self.market_mechanism(data[1])
        self.trading_mode(data[2])
        self.transaction_category(data[3])
        self.negotiated_trade(data[4])
        self.crossing_trade(data[5])
        self.modification_indicator(data[6])
        self.benchmark_indicator(data[7])
        self.ex_cum_dividend(data[8])
        self.publication_indicator(data[9])
        self.offbook_automated_indicator(data[10])

    # Login message
    @process_msg_header(20)
    def msg_login(self, data):
        names = (
            ('login_session_sub_id', 4),
            ('login_username', 4),
            ('login_filler', 2),
            ('login_password', 10),
        )
        return data.get_fields(names)

    # Login response message
    @process_msg_header(1)
    def msg_login_response(self, data):
        names = (
            ('flags', 1),
        )
        fields = data.get_fields(names)
        self.flags.login_status = {
            'A': 1, 'N': 2, 'B': 3, 'S': 4
        }.get(fields['flags'], 0)
        return {}

    # Gap request message
    @process_msg_header(7)
    def msg_gap_request(self, data):
        names = (
            ('gap_unit', 1),
            ('gap_sequense', 4),
            ('gap_count', 2),
        )
        return data.get_fields(names)

    # Gap response message
    @process_msg_header(8)
    def msg_gap_response(self, data):
        names = (
            ('gap_unit', 1),
            ('gap_sequence', 4),
            ('gap_count', 2),
            ('flags', 1),
        )
        fields = data.get_fields(names)

        self.flags.gap_status = {
            'A': 1, 'O': 2, 'D': 3, 'M': 4, 'S': 5, 'C': 6, 'I': 7
        }.get(fields['flags'], 0)

        return fields

    # Time message
    @process_msg_header(4)
    def msg_time(self, data):
        names = (
            ('pitch_time', 4),
        )
        fields = data.get_fields(names)
        self.pitch_time = fields['pitch_time']
        return fields

    # Unit Clear Message
    @process_msg_header(4)
    def msg_clear(self, data):
        names = (
            ('pitch_time_offset', 4),
        )
        return data.get_fields(names)

    # Add Order Message
    @process_msg_header(23)
    def msg_add_order(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
            ('pitch_side', 1),
            ('pitch_shares_s', 2),
            ('pitch_symbol', 6),
            ('pitch_price_s', 2)
        )
        return data.get_fields(names)

    # Add Order Message — Long Form
    @process_msg_header(33)
    def msg_add_order_long(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
            ('pitch_side', 1),
            ('pitch_shares_l', 4),
            ('pitch_symbol', 8),
            ('pitch_price_l', 8)
        )
        return data.get_fields(names)

    # Add Order Message — Expanded Form
    @process_msg_header(38)
    def msg_add_order_exp(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
            ('pitch_side', 1),
            ('pitch_share_ls', 4),
            ('pitch_symbol', 8),
            ('pitch_price_l', 8),
            ('pitch_add_order_flags', 1),
            ('pitch_participant', 4)
        )
        # TODO: Order flags = 1byte
        return data.get_fields(names)

    # Executed Order Message
    @process_msg_header(27)
    def msg_order_executed(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
            ('pitch_shares_l', 4),
            ('pitch_execution_id', 8),
            ('flags', 3)
        )
        fields = data.get_fields(names)

        self.parse_order_execution_flag(fields['flags'])

        return fields

    # Executed Order Price/Size Message
    @process_msg_header(39)
    def msg_order_executed_price(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
            ('pitch_e_shares_l', 4),
            ('pitch_r_shares_l', 4),
            ('pitch_execution_id', 8),
            ('pitch_price_l', 8),
            ('flags', 3)
        )
        fields = data.get_fields(names)

        self.parse_order_execution_flag(fields['flags'])

        return fields

    # Reduce Order Message
    @process_msg_header(14)
    def msg_reduce_size_short(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
            ('pitch_shares_s', 2),
        )
        return data.get_fields(names)

    # Reduce Order Message — Long Form
    @process_msg_header(16)
    def msg_reduce_size_long(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
            ('pitch_shares_l', 4),
        )
        return data.get_fields(names)

    # Modify Order Message — Short Form
    @process_msg_header(16)
    def msg_modify_order_short(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
            ('pitch_shares_s', 2),
            ('pitch_price_s', 2),
        )
        return data.get_fields(names)

    # Modify Order Message — Long Form
    @process_msg_header(24)
    def msg_modify_order_long(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
            ('pitch_shares_l', 4),
            ('pitch_price_l', 8),
        )
        return data.get_fields(names)

    # Delete Order Message
    @process_msg_header(12)
    def msg_delete_order(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
        )
        return data.get_fields(names)

    # Trade Message
    @process_msg_header(35)
    def msg_trade_short(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
            ('pitch_side', 1),
            ('pitch_shares_s', 2),
            ('pitch_symbol', 6),
            ('pitch_price_s', 2),
            ('pitch_execution_id', 8),
            ('flags', 4),
        )
        fields = data.get_fields(names)

        self.parse_trade_flags(fields['flags'])

        return fields

    # Trade Message - Long form
    @process_msg_header(45)
    def msg_trade_long(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_order', 8),
            ('pitch_side', 1),
            ('pitch_shares_l', 4),
            ('pitch_symbol', 8),
            ('pitch_price_l', 8),
            ('pitch_execution_id', 8),
            ('flags', 4),
        )
        fields = data.get_fields(names)

        self.parse_trade_flags(fields['flags'])

        return fields

    # Trade Break Message
    @process_msg_header(12)
    def msg_trade_break(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_execution_id', 8),
        )
        return data.get_fields(names)

    # Trade Report Message
    @process_msg_header(62)
    def msg_trade_report(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_shares_ll', 8),
            ('pitch_symbol', 8),
            ('pitch_price_l', 8),
            ('pitch_trade', 8),
            ('pitch_trade_timestamp', 8),
            ('pitch_exec_venue', 4),
            ('pitch_currency', 3),
            ('flags', 11)
        )
        fields = data.get_fields(names)

        # TODO: AY, Should be implemented
        """
        midnight = mktime(
            datetime.strptime(fields['date'], "%Y%m%d").timetuple()
        )
        fields['tradetime'] = self.date_format(midnight + int(fields['time']))

        del fields['date']
        del fields['time']
        """

        self.parse_trade_report_flags(fields['flags'])

        return fields

    # End Session Message
    @process_msg_header(4)
    def msg_end_session(self, data):
        names = (
            ('pitch_time_offset', 4),
        )
        return data.get_fields(names)

    # Trading Status Message
    @process_msg_header(21)
    def msg_trading_status(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_symbol', 8),
            ('flags', 1),
            ('pitch_reserved', 3)
        )
        fields = data.get_fields(names)
        self.flags.trading_status = {
            'T': 1, 'R': 2, 'C': 3, 'S': 4, 'N': 5, 'V': 6, 'O': 7,
            'E': 8, 'H': 9, 'M': 10, 'P': 11
        }.get(fields['flags'], 0)
        return fields

    # Statistics Message
    @process_msg_header(22)
    def msg_statistics(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_symbol', 8),
            ('pitch_price_l', 8),
            ('flags', 1),
            ('price_determination', 1)
        )
        fields = data.get_fields(names)

        self.flags.statistic_type = {
            'C': 1, 'H': 2, 'L': 3, 'O': 4, 'P': 5
        }.get(fields['flags'], 0)

        self.flags.pitch_price_determination = {
            '0': 1, '1': 2
        }.get(fields['price_determination'], 0)
        return fields

    # Auction Update Message
    @process_msg_header(45)
    def msg_auction_update(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_symbol', 8),
            ('auction_type', 1),
            ('pitch_reference_price_l', 8),
            ('pitch_buy_shares_l', 4),
            ('pitch_sell_shares_l', 4),
            ('pitch_indicative_price_l', 8),
            ('pitch_reserved', 8),
        )
        fields = data.get_fields(names)
        self.flags.auction_type = {
            'O': 1, 'C': 2, 'H': 3, 'V': 4
        }.get(fields['flags'], 0)
        return fields

    # Auction Summary Message
    @process_msg_header(47)
    def msg_auction_summary(self, data):
        names = (
            ('pitch_time_offset', 4),
            ('pitch_symbol', 8),
            ('auction_type', 1),
            ('pitch_price_l', 8),
            ('pitch_shares_l', 4)
        )
        fields = data.get_fields(names)
        self.flags.auction_type = {
            'O': 1, 'C': 2, 'H': 3, 'V': 4
        }.get(fields['flags'], 0)
        return fields

    def __init__(self, name, **params):
        self.types = {
            0x01: self.msg_login,
            0x02: self.msg_login_response,
            0x03: self.msg_gap_request,
            0x04: self.msg_gap_response,
            0x20: self.msg_time,
            0x97: self.msg_clear,
            0x22: self.msg_add_order,
            0x40: self.msg_add_order_long,
            0x2f: self.msg_add_order_exp,
            0x23: self.msg_order_executed,
            0x24: self.msg_order_executed_price,
            0x25: self.msg_reduce_size_long,
            0x26: self.msg_reduce_size_short,
            0x27: self.msg_modify_order_long,
            0x28: self.msg_modify_order_short,
            0x29: self.msg_delete_order,
            0x2b: self.msg_trade_short,
            0x41: self.msg_trade_long,
            0x2c: self.msg_trade_break,
            0x32: self.msg_trade_report,
            0x2d: self.msg_end_session,
            0x31: self.msg_trading_status,
            0x34: self.msg_statistics,
            0x95: self.msg_auction_update,
            0x96: self.msg_auction_summary
            # TODO: Add spin message parsing
        }

        self.date = date.today()

    """
    Name            Offset  Length      Description
    Hdr Length      0       2 Binary    Length of entire block
                                        of messages. Includes
                                        this header and “Hdr Count”
                                        messages to follow.
    Hdr Count       1       1 Binary    Number of messages to follow
                                        this header.
    Hdr Unit        1       1 Binary    Unit that applies to messages
                                        included in this header.
    Hdr Sequence    4       4 Binary    Sequence of first message to
                                        follow this header.
    """
    def parse_sequence_header(self, data):
        try:
            return unpack("HBBI", data)
        except unpack_error:
            # End of stream
            return 0, 0, 0, 0

    """
    Parse data entry point
    @param      bytes_data, RAW data to parse
    """
    def parse(self, bytes_data):

        self.fields = []
        self.midnight = mktime(datetime.today().timetuple())

        data = bytes_data
        if not data:
            return

        while True:
            seq_len, msg_count, unit, seq = \
                self.parse_sequence_header(data[0:8])

            if not seq_len:
                break

            # print("Start sequence:", seq)
            offset = 8
            for i in range(0, msg_count):
                self.contract = seq
                mlen, mtype = unpack("BB", data[offset:offset+2])

                # process message body
                if mtype in self.types:
                    self.types[mtype](data[offset+2:offset+mlen])
                offset += mlen
            data = data[seq_len:]

            # closing quotes, also flush rows...
            self.close_quotes()

    def map_quote(self, fields):

        """
        BATS                           LOB
        gap_count                   =>
        gap_sequense                =>
        gap_unit                    =>

        login_filler                =>
        login_password              =>
        login_session_sub_id        =>
        login_status                =>
        login_username              =>

        pitch_price_s               => price
        pitch_price_l               => price
        pitch_reference_price_l     =>
        pitch_indicative_price_l    =>
        pitch_price_l               => price
        pitch_shares_s              => size
        pitch_shares_l              => size
        pitch_shares_ll             => size
        pitch_buy_shares_l          =>
        pitch_sell_shares_l         =>
        pitch_side                  => side
        pitch_time       \
        pitch_time_offset           => timestamp =
            midnight + pitch_time + pitch_time_offset
        pitch_order                 =>
        pitch_participant           =>
        pitch_price_determination   =>
        pitch_statistic_type        =>
        pitch_trading_status        =>
        pitch_symbol                =>
        pitch_currency              =>
        pitch_exec_venue            =>
        pitch_execution_id          =>
        pitch_reserved              =>
        pitch_statistic_type        =>
        pitch_symbol                =>
        pitch_trade                 =>
        pitch_trade_timestamp       =>

        benchmark_indicator         =>
        crossing_trade              =>
        ex_cum_dividend             => ex cum_dividend
        market_mechanism            => market_mechanism
        modification_indicator      =>
        negotiated_trade            =>
        offbook_automated_indicator =>
        publication_indicator       =>
        trade_timing_indicator      =>
        trading_mode                => trading_mode
        transaction_category        => transaction_category
        """

        # TODO: Map fields described above to quote

        # ?? what field is contract in BATS
        contract = ""

        def get_ts():
            dt = datetime.now()
            if 'pitch_time_offset' in fields:
                dt = self.date_format(
                    self.midnight +
                    unpack("I", self.pitch_time)[0] +
                    unpack("I", fields['pitch_time_offset'])[0]/1000
                )
            return dt

        entry = {}

        def map_entry(src, dst, fconv, store=1):
            if src in fields:
                # print("%s => %s"%(src, dst), self.to_bstr(fields[src]))
                if not store:
                    return fconv(fields[src])
                entry[dst] = fconv(fields[src])

        map_entry('pitch_shares_s', 'size', lambda x: unpack("H", x)[0])
        map_entry('pitch_shares_l', 'size', lambda x: unpack("I", x)[0])
        map_entry('pitch_shares_ll', 'size', lambda x: unpack("Q", x)[0])

        # map_entry('pitch_buy_shares_l', 'size', lambda x: unpack("Q", x)[0])
        # map_entry('pitch_sell_shares_l', 'size', lambda x: unpack("Q", x)[0])

        map_entry('pitch_price_l', 'price',
                  lambda x: float(unpack("Q", x)[0] / 10000))
        map_entry('pitch_price_s', 'price',
                  lambda x: float(unpack("H", x)[0] / 100))

        map_entry('pitch_side', 'side', lambda x: {b'B': 0, b'S': 1}.get(x))

        contract = self.contract

        # f = [k for k, v in fields.items() if k not in self.fields]
        # self.fields += f
        # print("Store:", ", ".join(
        #     ["%s => %s:%s"%(k, type(v), str(v))for k,v in entry.items()])
        # )

        entry['flags'] = self.flags

        return contract, get_ts(), entry, ""

    @staticmethod
    def date_format(stamp):
        try:
            return datetime.fromtimestamp(stamp)
        except (IndexError, ValueError):
            # Log or raise exception
            return None
