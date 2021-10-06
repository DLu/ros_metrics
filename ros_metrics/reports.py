import collections
import datetime

from .util import epoch_to_datetime, year_month_to_datetime

ONE_WEEK = datetime.timedelta(days=7)


def get_datetime_from_dict(row, time_field):
    if time_field == 'year, month':
        return year_month_to_datetime(row['year'], row['month'])
    else:
        return epoch_to_datetime(row[time_field])


def round_time(dt, mode=None):
    if mode is None:
        mode = ('weeks', 2)

    regular = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if mode == 'week':
        return regular - datetime.timedelta(days=regular.weekday())
    elif isinstance(mode, tuple) and mode[0] == 'weeks':
        num_weeks = mode[1]  # Number of weeks to round to
        week = regular - datetime.timedelta(days=regular.weekday())  # Rounded to beginning of week
        _, weeknumber, _ = week.isocalendar()  # Get week number
        off = weeknumber % num_weeks  # Get number of weeks off from rounded value
        return week - datetime.timedelta(days=7 * off)
    elif mode == 'month':
        return regular - datetime.timedelta(days=regular.day)
    else:
        raise NotImplementedError(f'Unsupported mode {mode}')


def round_series(all_values, mode=None):
    series = []
    last_time = None
    last_val = 0
    for dt, count in all_values:
        regular = round_time(dt, mode)
        if last_time != regular:
            series.append((regular, last_val))
            last_time = regular
        last_val = count

    return series


def get_series(db, table, time_field, value_field, clause=''):
    series = []
    query = f'SELECT {value_field}, {time_field} FROM {table} {clause} ORDER BY {time_field}'
    for row in db.query(query):
        series.append((get_datetime_from_dict(row, time_field), row[value_field]))
    return series


def get_aggregate_series(db, table, time_field, resolution=ONE_WEEK, clause=''):
    series = []
    query = f'SELECT {time_field} FROM {table} WHERE {time_field} is not NULL {clause} ORDER BY {time_field}'
    last_time = None
    count = 0
    for row in db.query(query):
        count += 1
        dt = get_datetime_from_dict(row, time_field)
        if resolution is None or last_time is None or dt - last_time > resolution:
            last_time = dt
            series.append((dt, count))
    return series


def get_regular_aggregate_series(db, table, time_field, mode=None, clause=''):
    return round_series(get_aggregate_series(db, table, time_field, resolution=None, clause=clause), mode)


def get_unique_series(db, table, time_field, ident_field, resolution=ONE_WEEK):
    series = []
    seen = set()
    query = f'SELECT {time_field}, {ident_field} FROM {table} WHERE {time_field} is not NULL ORDER BY {time_field}'
    last_time = None
    for row in db.query(query):
        ident = row[ident_field]
        if ident in seen:
            continue
        seen.add(ident)
        dt = get_datetime_from_dict(row, time_field)
        if resolution is None or last_time is None or dt - last_time > resolution:
            last_time = dt
            series.append((dt, len(seen)))
    return series


def get_regular_unique_series(db, table, time_field, ident_field, mode=None):
    return round_series(get_unique_series(db, table, time_field, ident_field, resolution=None), mode)


def order_by_magnitude(count_dict, remove_fields=[]):
    ordered_values = []
    for k, v in sorted(count_dict.items(), key=lambda item: item[1], reverse=True):
        if k.lower() in remove_fields:
            continue
        ordered_values.append(k)
    return ordered_values


def time_buckets(db, table, values, time_field, ident_field, value_field=None, months=True):
    buckets = collections.defaultdict(collections.Counter)
    for value in values:
        select_field = time_field
        if value_field:
            select_field += ', ' + value_field

        one_time_field = time_field.split(',')[0]
        results = db.query(f'SELECT {select_field} FROM {table} '
                           f"WHERE {ident_field} = '{value}' AND {one_time_field} IS NOT NULL ORDER BY {time_field}")
        for result in results:
            dt = get_datetime_from_dict(result, time_field)
            if months:
                key = dt.year, dt.month
            else:
                key = dt.year
            if value_field is None:
                buckets[key][value] += 1
            elif result.get(value_field):
                buckets[key][value] += result[value_field]
    return buckets


def normalize_timepoints(series_dict, values=None, round_places=4):
    plots = collections.defaultdict(list)
    totals = collections.defaultdict(int)

    needs_sort = False
    if values is None:
        values = set()
        for d in series_dict.values():
            values.update(d.keys())
        values = list(values)
        needs_sort = True

    for ym, line in sorted(series_dict.items()):
        total = float(sum(line.values()))
        dt = year_month_to_datetime(*ym)

        for value in values:
            v = line.get(value, 0) / total
            if round_places is not None:
                v = round(v, round_places)
            plots[value].append((dt, v))
            totals[value] += v

    if needs_sort:
        values = sorted(values, key=lambda value: totals[value], reverse=True)

    final_plots = collections.OrderedDict()
    for value in values:
        if totals[value] <= 0.0:
            continue
        final_plots[value] = plots[value]
    return final_plots


def get_email_plots(db):
    total = collections.Counter()
    unique = collections.Counter()
    seen = set()

    today = datetime.datetime.today()
    today_key = today.year, today.month

    results = db.query('SELECT created_at, topic_id FROM posts WHERE created_at IS NOT NULL ORDER BY created_at')
    for result in results:
        dt = get_datetime_from_dict(result, 'created_at')
        key = dt.year, dt.month
        if key == today_key:
            continue
        total[key] += 1
        ident = result['topic_id']
        if ident in seen:
            continue
        seen.add(ident)
        unique[key] += 1

    return total, unique


def buckets_to_plot(buckets):
    series = []
    for ym, value in sorted(buckets.items()):
        dt = year_month_to_datetime(*ym)
        series.append((dt, value))
    return series


def get_top_by_year(db, table, ident_field, value_field, clause='', yearly_count=15, all_time_count=15,
                    ident_tranformer=None):
    earliest = {}
    total = collections.Counter()

    for row in db.query(f'SELECT {ident_field}, {value_field}, year FROM {table} {clause}'):
        ident = row[ident_field]
        if ident_tranformer:
            ident = ident_tranformer(ident)
        if not ident:
            continue
        if ident in earliest:
            earliest[ident] = min(earliest[ident], row['year'])
        else:
            earliest[ident] = row['year']
        total[ident] += row[value_field]

    yearly = collections.defaultdict(list)
    years = sorted(set(earliest.values()))
    for year in years[1:]:
        pkgs = [pkg for pkg in earliest if earliest[pkg] == year]
        for pkg, hits in total.most_common():
            if pkg in pkgs:
                yearly[year].append((pkg, hits))
                if len(yearly[year]) >= yearly_count:
                    break
    all_time = list(total.most_common(all_time_count))
    return all_time, yearly
