/**
 * Helper function to get relative position for an event - copied from Chart.js
 * @param {Event|IEvent} event - The event to get the position for
 * @param {Chart} chart - The chart
 * @returns {object} the event position
 */
function getRelativePosition(e, chart) {
    if (e.native) {
        return {
            x: e.x,
            y: e.y
        };
    }
    return Chart.helpers.getRelativePosition(e, chart);
}


Chart.Interaction.modes['xfirst'] =
    function(chart, e, options) {
        var position = getRelativePosition(e, chart);
        var items = [];
        var intersectsItem = false;

        var datasets = chart.data.datasets;
        for (var i = 0; i < chart.data.datasets.length; ++i)
        {
            if (!chart.isDatasetVisible(i)) {
                continue;
            }
            var meta = chart.getDatasetMeta(i);
            for (var j = 0; j < meta.data.length; ++j) {
                var element = meta.data[j];
                if (element._view.skip) { continue; }

                if (element.inRange(position.x, position.y))
                {
                    intersectsItem = true;
                }

                if (element.inXRange(position.x))
                {
                    items.push(element);
                    break;
                }
            }
        }

        // If we want to trigger on an intersect and we don't have any items
        // that intersect the position, return nothing
        if (options.intersect && !intersectsItem) {
            items = [];
        }
        return items;
    }
