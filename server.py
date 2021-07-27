from flask import Flask, g, jsonify, request
import os
import psycopg2
from flask_cors import CORS
from datetime import datetime, timedelta


def create_app(debug=False):
    """Create an application."""
    app = Flask(__name__)
    app.debug = debug

    print('debug mode: ' + str(app.debug))

    return app


app = create_app()
cors = CORS(app)


def cached_jsonify(data, seconds=60):
    response = jsonify(data)
    response.headers['Cache-Control'] = f'public, max-age={seconds}'

    return response

# TODO: refactor - remove duplicated functions
def connect_transactions_db():
    """Connects to the specific database."""

    conn = psycopg2.connect(os.environ.get('TRANSACTIONS_DATABASE_URL'))

    return conn


def get_transactions_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'transactions_db'):
        g.pg_db = connect_transactions_db()
    return g.pg_db


def connect_trades_db():
    """Connects to the specific database."""

    conn = psycopg2.connect(os.environ.get('TRADES_DATABASE_URL'))

    return conn


def get_trades_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'trades_db'):
        g.pg_db = connect_trades_db()
    return g.pg_db

@app.route('/stats/activity_feed/<margin_account>')
def activity_feed(margin_account):
    try:

        db = get_transactions_db()
        cur = db.cursor()

        limit = request.args.get('limit')
        offset = request.args.get('offset')
        if limit is None:
            limit = 10_000 # default limit
            offset = 0
        else:
            if offset is None:
                offset = 0

        sql = """
        select array_to_json(array_agg(row_to_json(ordered_u))) from
        (
        select * from 
        (
        select 'Withdraw' as activity_type, w.block_datetime, row_to_json(w) as activity_details from
        (
        select 
        dw.margin_account, dw.signature, dw.owner, dw.symbol, dw.quantity, dw.usd_equivalent, dw.block_datetime, dw.mango_group 
        from deposit_withdraw dw
        where 
        dw.margin_account = %(margin_account)s 
        and dw.side = 'Withdraw'
        ) w
        union all
        select 'Deposit' as activity_type, d.block_datetime, row_to_json(d) as activity_details from
        (
        select 
        dw.margin_account, dw.signature, dw.owner, dw.symbol, dw.quantity, dw.usd_equivalent, dw.block_datetime, dw.mango_group 
        from deposit_withdraw dw
        where 
        dw.margin_account = %(margin_account)s 
        and dw.side = 'Deposit'
        ) d
        union all
        select 'Liquidation' as activity_type, ld.block_datetime, row_to_json(ld) as activity_details from
        (
        select l.*,
            (
            select array_to_json(array_agg(row_to_json(lh)))
            from (
                select symbol, start_assets, start_liabs, end_assets, end_liabs, price 
                from liquidation_holdings 
                where signature = l.signature 
            ) lh
            ) as balances
    
        from liquidations l 
        where l.liqee  = %(margin_account)s
        ) ld
        ) u
        order by u.block_datetime desc
        limit %(limit)s offset %(offset)s
        ) ordered_u
        """

        cur.execute(sql, {'margin_account': margin_account, 'limit': limit, 'offset': offset})
        data = cur.fetchone()[0]

        if data is None:
            return jsonify([])
        else:
            return jsonify(data)

    except Exception as e:
        print(e)


@app.route('/stats/withdraws/<margin_account>')
def withdraws(margin_account):
    try:

        # margin_account = 'FKCBDQwmTj6HeJ1uU93go7xcUN2XX1myeHyzfK5iAj3X'

        db = get_transactions_db()
        cur = db.cursor()

        limit = request.args.get('limit')
        offset = request.args.get('offset')
        if limit is None:
            limit = 10_000 # default limit
            offset = 0
        else:
            if offset is None:
                offset = 0


        sql = """
        select array_to_json(array_agg(row_to_json(d))) from
        (
        select 
        dw.margin_account, dw.signature, dw.owner, dw.symbol, dw.quantity, dw.usd_equivalent, dw.block_datetime, dw.mango_group 
        from deposit_withdraw dw
        where 
        dw.margin_account = %(margin_account)s 
        and dw.side = 'Withdraw'
        order by dw.block_datetime desc
        limit %(limit)s offset %(offset)s
        ) d
        """

        cur.execute(sql, {'margin_account': margin_account, 'limit': limit, 'offset': offset})
        data = cur.fetchone()[0]

        if data is None:
            return jsonify([])
        else:
            return jsonify(data)

    except Exception as e:
        print(e)

@app.route('/stats/deposits/<margin_account>')
def deposits(margin_account):
    try:

        # margin_account = 'HmrkFSrqnECzFgENsiAsCQ8TzCfCyDz8oUuMtZzmSaAj'

        db = get_transactions_db()
        cur = db.cursor()

        limit = request.args.get('limit')
        offset = request.args.get('offset')
        if limit is None:
            limit = 10_000 # default limit
            offset = 0
        else:
            if offset is None:
                offset = 0


        sql = """
        select array_to_json(array_agg(row_to_json(d))) from
        (
        select 
        dw.margin_account, dw.signature, dw.owner, dw.symbol, dw.quantity, dw.usd_equivalent, dw.block_datetime, dw.mango_group 
        from deposit_withdraw dw
        where 
        dw.margin_account = %(margin_account)s 
        and dw.side = 'Deposit'
        order by dw.block_datetime desc
        limit %(limit)s offset %(offset)s
        ) d
        """

        cur.execute(sql, {'margin_account': margin_account, 'limit': limit, 'offset': offset})
        data = cur.fetchone()[0]

        if data is None:
            return jsonify([])
        else:
            return jsonify(data)

    except Exception as e:
        print(e)


@app.route('/stats/liquidations/<margin_account>')
def liquidations(margin_account):
    try:

        # margin_account = 'FucJ8CAfqSVuPr2zGhDxjyxkYvb5Qd1Maqqbc5JrPbYb'

        db = get_transactions_db()
        cur = db.cursor()

        limit = request.args.get('limit')
        offset = request.args.get('offset')
        if limit is None:
            limit = 10_000 # default limit
            offset = 0
        else:
            if offset is None:
                offset = 0

        sql = """
        select array_to_json(array_agg(row_to_json(out))) from 
        (
            select l.*,
                (
                select array_to_json(array_agg(row_to_json(lh)))
                from (
                    select symbol, start_assets, start_liabs, end_assets, end_liabs, price 
                    from liquidation_holdings 
                    where signature = l.signature 
                ) lh
                ) as balances
    
            from liquidations l 
            where l.liqee  = %(margin_account)s
            order by l.block_datetime desc
            limit %(limit)s offset %(offset)s
        ) out
        """

        cur.execute(sql, {'margin_account': margin_account, 'limit': limit, 'offset': offset})
        data = cur.fetchone()[0]

        if data is None:
            return jsonify([])
        else:
            return jsonify(data)

    except Exception as e:
        print(e)


@app.route('/stats/all_liquidations')
def all_liquidations():
    try:
        db = get_transactions_db()
        cur = db.cursor()

        limit = request.args.get('limit')
        offset = request.args.get('offset')
        if limit is None:
            limit = 10_000  # default limit
            offset = 0
        else:
            if offset is None:
                offset = 0

        sql = """
        select array_to_json(array_agg(row_to_json(out))) from 
        (
            select l.*,
                (
                select array_to_json(array_agg(row_to_json(lh)))
                from (
                    select symbol, start_assets, start_liabs, end_assets, end_liabs, price 
                    from liquidation_holdings 
                    where signature = l.signature 
                ) lh
                ) as balances
            from liquidations l 
            order by l.block_datetime desc
            limit %(limit)s offset %(offset)s
        ) out
        """

        cur.execute(sql, {'limit': limit, 'offset': offset})
        data = cur.fetchone()[0]

        if data is None:
            return jsonify([])
        else:
            return jsonify(data)

    except Exception as e:
        print(e)

@app.route('/stats/prices/<mango_group>')
def prices(mango_group):
    try:

        # mango_group = '2oogpTYm1sp6LPZAWD3bp2wsFpnV2kXL1s52yyFhW5vp'

        db = get_transactions_db()
        cur = db.cursor()

        # TODO - think about optimising this more (indexes) - and caching
        sql = """
        select json_object_agg(symbol, date_prices) from
        (
        select symbol, json_object_agg(date_hour_formatted, price) as date_prices
        from
        (
        -- fill in null values with last available price before hour
        select om.symbol, t4.date_hour, to_char(t4.date_hour, 'YYYY-MM-DD"T"HH24:MI:SS".000Z"') as date_hour_formatted, ot3.submit_value / power(10, om.decimals) as price
        from 
        (
        select oracle_pk, date_hour, max_block_datetime, first_value(max_block_datetime) over (partition by oracle_pk, group_id order by date_hour) as corrected_max_block_datetime
        from 
        (
        select t1.oracle_pk, hc.date_hour, max(ot2.block_datetime) as max_block_datetime, sum(case when max(ot2.block_datetime) is not null then 1 end) over (order by hc.date_hour) as group_id
        from
        (
        select 
        ot.oracle_pk, date_trunc('hour', min(block_datetime)) as min_block_datetime, date_trunc('hour', max(block_datetime)) as max_block_datetime
        from oracle_transactions ot 
        inner join mango_group_oracles mgo 
        on mgo.oracle_pk = ot.oracle_pk 
        where
        mgo.mango_group_pk = %(mango_group)s
        group by ot.oracle_pk
        ) t1
        inner join hourly_calendar hc 
        on hc.date_hour between t1.min_block_datetime and t1.max_block_datetime
        left join
        oracle_transactions ot2 
        on ot2.oracle_pk = t1.oracle_pk
        and date_trunc('hour', ot2.block_datetime) = hc.date_hour 
        group by t1.oracle_pk, hc.date_hour 
        order by t1.oracle_pk , hc.date_hour 
        ) t3
        ) t4
        inner join 
        oracle_transactions ot3 
        on ot3.oracle_pk = t4.oracle_pk
        and ot3.block_datetime  = t4.corrected_max_block_datetime
        inner join oracle_meta om 
        on om.oracle_pk = t4.oracle_pk
        ) t5
        group by symbol
        ) t6
        """

        cur.execute(sql, {'mango_group': mango_group})
        data = cur.fetchone()[0]

        if data is None:
            return jsonify({})
        else:
            return jsonify(data)

    except Exception as e:
        print(e)


@app.route('/stats/pnl_leaderboard')
def pnl_leaderboard():
    try:

        db = get_trades_db()
        cur = db.cursor()

        start_date = request.args.get('start_date')
        if start_date is None:
            start_date = '1900-01-01'

        limit = request.args.get('limit')
        offset = request.args.get('offset')
        if limit is None:
            limit = 10_000 # default limit
            offset = 0
        else:
            if offset is None:
                offset = 0

        # TODO - think about optimising this more (indexes) - and caching
        sql = """
        select array_to_json(array_agg(row_to_json(t))) from 
        (
        select 
        pc.margin_account,
        pc.owner,
        case when pc.name is null then '' else pc.name end as name,
        pc.cumulative_pnl - case when pc2.cumulative_pnl is null then 0 else pc2.cumulative_pnl end as pnl,
        row_number() over (order by pc.cumulative_pnl - case when pc2.cumulative_pnl is null then 0 else pc2.cumulative_pnl end desc) as rank
        from pnl_cache pc
        left join pnl_cache pc2 
        on pc2.margin_account = pc.margin_account 
        and pc2.price_date = (%(start_date)s ::date - interval '1 day')::date
        where pc.price_date = (select max(price_date) from prices)
        order by
        pc.cumulative_pnl - case when pc2.cumulative_pnl is null then 0 else pc2.cumulative_pnl end desc
        limit %(limit)s offset %(offset)s
        ) t
        """

        cur.execute(sql, {'start_date': start_date, 'limit': limit, 'offset': offset})
        data = cur.fetchone()[0]

        if data is None:
            return cached_jsonify({})
        else:
            return cached_jsonify(data)

    except Exception as e:
        print(e)


@app.route('/stats/pnl_leaderboard_rank/<margin_account>')
def pnl_leaderboard_rank(margin_account):
    try:

        db = get_trades_db()
        cur = db.cursor()

        start_date = request.args.get('start_date')
        if start_date is None:
            start_date = '1900-01-01'

        sql = """
        select row_to_json(t) from 
        (
        select margin_account, owner, name, pnl, rank from
            (
            select 
            pc.margin_account,
            pc.owner,
            case when pc.name is null then '' else pc.name end as name,
            pc.cumulative_pnl - case when pc2.cumulative_pnl is null then 0 else pc2.cumulative_pnl end as pnl,
            row_number() over (order by pc.cumulative_pnl - case when pc2.cumulative_pnl is null then 0 else pc2.cumulative_pnl end desc) as rank
            from pnl_cache pc
            left join pnl_cache pc2 
            on pc2.margin_account = pc.margin_account 
            and pc2.price_date = (%(start_date)s ::date - interval '1 day')::date
            where pc.price_date = (select max(price_date) from prices)
        ) t1
        where margin_account = %(margin_account)s
        ) t
        """

        cur.execute(sql, {'start_date': start_date, 'margin_account': margin_account})
        data = cur.fetchone()

        if data is None:
            return cached_jsonify({})
        else:
            return cached_jsonify(data[0])

    except Exception as e:
        print(e)


@app.route('/stats/pnl_history/<margin_account>')
def pnl_history(margin_account):
    try:

        db = get_trades_db()
        cur = db.cursor()

        start_date = request.args.get('start_date')

        limit = request.args.get('limit')
        offset = request.args.get('offset')
        if limit is None:
            limit = 10_000 # default limit
            offset = 0
        else:
            if offset is None:
                offset = 0

        sql = """
        select array_to_json(array_agg(row_to_json(t))) from 
        (
        select 
        margin_account, 
        owner, 
        case when pc.name is null then '' else pc.name end as name,
        price_date as date, 
        cumulative_pnl
        from pnl_cache pc 
        where margin_account = %(margin_account)s
        order by price_date desc
        limit %(limit)s offset %(offset)s
        ) t
        """

        cur.execute(sql, {'margin_account': margin_account, 'limit': limit, 'offset': offset})
        data = cur.fetchone()[0]

        if data is None:
            return jsonify({})
        else:

            if start_date is not None:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()

                last_entry = data[-1]
                last_dt = datetime.strptime(last_entry['date'], '%Y-%m-%d').date()

                # Pad PNL history with 0's if needed
                if last_dt >= start_dt:
                    owner = last_entry['owner']
                    name = last_entry['name']

                    delta = timedelta(days=1)
                    dt_iter = last_dt - delta
                    while dt_iter >= start_dt:
                        data.append({
                            'margin_account': margin_account,
                            'owner': owner,
                            'name': name,
                            'date': dt_iter.strftime('%Y-%m-%d'),
                            'cumulative_pnl': 0
                        })
                        dt_iter -= delta

            return cached_jsonify(data)

    except Exception as e:
        print(e)


@app.route('/')
def index():
    return "<h1>Welcome to mango transaction stats</h1>"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('SERVER_PORT')))

