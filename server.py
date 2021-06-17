from flask import Flask, g, jsonify, request
# from flask_cors import CORS
import os
import psycopg2


def create_app(debug=False):
    """Create an application."""
    app = Flask(__name__)
    app.debug = debug

    print('debug mode: ' + str(app.debug))

    return app

app = create_app()
# cors = CORS(app)

def connect_pg_db():
    """Connects to the specific database."""

    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))

    return conn

def get_pg_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'pg_db'):
        g.pg_db = connect_pg_db()
    return g.pg_db

@app.route('/stats/withdraws/<margin_account>')
def withdraws(margin_account):
    db = get_pg_db()
    cur = db.cursor()

    limit = request.args.get('limit')
    offset = request.args.get('offset')
    if limit is None:
        limit = 10_000 # default limit
        offset = 0
    else:
        if offset is None:
            offset = 0

    # margin_account = 'FKCBDQwmTj6HeJ1uU93go7xcUN2XX1myeHyzfK5iAj3X'

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

@app.route('/stats/deposits/<margin_account>')
def deposits(margin_account):

    db = get_pg_db()
    cur = db.cursor()

    limit = request.args.get('limit')
    offset = request.args.get('offset')
    if limit is None:
        limit = 10_000 # default limit
        offset = 0
    else:
        if offset is None:
            offset = 0

    # margin_account = 'HmrkFSrqnECzFgENsiAsCQ8TzCfCyDz8oUuMtZzmSaAj'

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
    
@app.route('/stats/liquidations/<margin_account>')
def liquidations(margin_account):

    # margin_account = 'FucJ8CAfqSVuPr2zGhDxjyxkYvb5Qd1Maqqbc5JrPbYb'

    db = get_pg_db()
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

@app.route('/stats/prices/<mango_group>')
def prices(mango_group):

    # mango_group = '2oogpTYm1sp6LPZAWD3bp2wsFpnV2kXL1s52yyFhW5vp'

    db = get_pg_db()
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=os.environ.get('SERVER_PORT'))
