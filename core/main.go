// Package pudb implements the apache pulsar io writing
package pudb

// as per gemini:
// org:
// tenants: orgs
// namespace: routing
// topics: pushed message url

// data:
// subscriptions
// shemas:

// integration:
// sources: import data
// sinks: export data
// packages: version control shit

// infra:
// brokers: handles the cluster traffic
// end

// how I view:
// ##note: here,think of "tenant"|"namespace"|"topic" as bucket,branch,object
// bucket -> core
// branch -> branch/folder....
// object -> object/data
// end

// architecure:
// focues on builidng the bucket,branch,object,data
// rather than each time pushing the url now you can just push
// the bucket,branch,object
