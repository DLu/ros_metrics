var ONE_DAY = 24 * 60 * 60;

function durationRenderer(data, type, row)
{
    if (type != "display") return data;
    var days = (data / ONE_DAY) | 0;
    var years = (days / 365) | 0;
    if (years > 0)
    {
        days -= years * 365;
        return years + ' years, ' + days + ' days';
    }
    return days + ' days';
}

function rankRenderer(data, type, row, meta)
{
    if (type != "display") return data;

    var rank = row[meta.col + 1];
    return data + ' <span class="rank_column">' + rank + "</span>";
}

function linkRenderer(data, type, row, meta)
{
    if (type != "display") return data;

    var org = row[meta.col - 1];
    return '<a href="repos/' + org + '_' + data + '.html">' + data + '</a>';
}
