# ros_metrics

## Publicly Available
### [ROS Users](http://lists.ros.org/pipermail/ros-users/)
 * Downloads `tar` archives
 * Sorts by Topic (roughly), Post and User
 * No method exists for getting historical number of subscribers
 * TODO: Use https://github.com/zapier/email-reply-parser to remove replies from raw text

### [answers.ros.org](http://answers.ros.org)
 * Uses [askbot API](https://github.com/ASKBOT/askbot-devel/blob/master/askbot/doc/source/api.rst) for info on users and questions.

### [packages.ros.org](https://awstats.osuosl.org/list/packages.ros.org)
 * Scrape HTML from the pages and gather
    * Overall traffic (hits, bandwidth, number of visitors)
    * Breakdown by url, country and operating system
 * Statistics by month/year.

### [rosdistro](https://github.com/ros/rosdistro/)
 * Examines Github repo
 * Determines type of change for each commit
 * Counts the number of repositories contained in the distributions per commit
 * Tracks which repos are present in each distro through time

### Multi-Repo Stats
 * Statistics based on the repositories listed in the rosdistro
 * Scores [Github repositories](https://developer.github.com/v3/) based on stars, forks and subscribers.
 * Gathers information on the issues/pull requests opened and closed.
 * Counts the total number of commits
 * TODO:
     * [ ] Determine number of packages per repository/commit
     * [ ] General git stats
        * lines of code
        * authors
        * languages

### [Google Scholar Citations](https://scholar.google.com/citations?view_op=view_citation&citation_for_view=fMDLYCUAAAAJ:u5HHmVD_uO8C)
 * Uses resources from [`scholarly` Python library](https://github.com/OrganicIrradiation/scholarly)
 * Counts citations per year

### [ROS Wiki](https://wiki.ros.org)
 * Uses a combination of the [publicly available mirror data](http://wiki.ros.org/Mirrors#Setup_rsync) and the [DocBook](http://moinmo.in/DocBook) format downloads.
 * Tracks the number of editors, pages and revisions.
 * Raw number of users is not publicly available and drawn from the official Metrics report.
 * TODO:
     * [ ] [Special case for ROS2](https://github.com/ros2/ros2_documentation)
     * [ ] How are wiki edits spread across users?
     * [ ] How big are wiki edits?
     * [ ] Number of wiki tutorial pages under in any package
     * [ ] Content per language/namespace
     * [ ] Integrate users

## Privately Available
The following data sources can only be crawled via use of an API key (or other special authentication)

### [discourse.ros.org](https://discourse.ros.org/)
 * Crawled using [Discourse API](https://docs.discourse.org/#tag/Categories%2Fpaths%2F~1categories.json%2Fget)
 * Retrieves info on
    * users
    * categories
    * topics
    * posts

### Web Traffic
 * Analytics available for
    * [wiki.ros.org](http://wiki.ros.org)
    * [answers.ros.org](http://answers.ros.org)
    * [discourse.ros.org](https://discourse.ros.org)
    * [index.ros.org](http://index.ros.org)
 * Uses [Google Analytics API](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/installed-py)
 * Overall number of unique pageviews, users and sessions gathered by month/year.
 * Pageviews breakdown by [url, country and operating system](https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/) done by year.
