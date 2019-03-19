# Social data fetcher
This is a reactive micro-service to collect users comments from social media and news channels. It can be easily integrated via `akka-remote`,
and relies on `akka-stream` to handle back-pressure and fault tolerance. Currently, cost-optimized queries 
to the Youtube Data API are implemented, but more data sources will be available soon, together with a more detailed documentation.  