# yarn-stats
It can be surprisingly hard to get an early warning if something on a hadoop cluster is causing slowdowns of jobs.
This project attempts to mine the yarn resourcemanager for information on currently running job health.
If we look at currently running and keep making snaphots we can compare these over time and spot if the percentiles
change drastically or not.

For now this is just a really simple example where we only look at RUNNING/ACCEPTED/SUBMITTED jobs from a user
called mapred and nothing else.