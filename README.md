# Introduction

**Work in progress** I'm currently untangling code and tests related to this codebase from our internal codebase. This is an ongoing project. The short term goal is to present this stuff at the Elasticsearch meetup end of January 2016. Beware, API stability is not a goal until we hit 1.0. There will be changes.

# Features

- Json http rest client (with get/put/post/delete support). Takes and returns jsonj's `JsonObject` instances.
- Rich EsApiClient that uses the http rest client. Loads of convenient methods to deal with document CRUD, searches, etc.
- BulkIndexer with support for inserts/deletes/updates, parallelism, error handling, and a call back API.
- ...

# Why and how

At Inbot we have been early adopters of Elasticsearch and we have used it for several years already. While we have Java as part of our architecture, we've always refrained from using the native java API that comes with elasticsearch for several reasons:

- it complicates deployment since now all your backend servers need to have elasticsearch nodes embedded and it prevents proper isolation of these two things
- most of the documentation assumes you use http
- the performance benefits are there but not as dramatic as some would have you believe. This is the same reason that e.g. Logstash no longer ships with embeded elasticsearch as a default option. If you use http correctly, it is mostly the same amount of stuff going over the wire. Our library does the right thing by not needlessly buffering requests/responses, by pooling connections using httpclient, and by supporting gzip compression (if you set up http client to do this).

So, we used HTTP to talk to Elasticsearch from day one and started out with some simple code around apache http client that over the years grew into a mini framework with loads of features. I always had the plan to open source this but never got around to cleaning it up. Recently the release of Elasticsearch 2.0 forced me to finally refactor the code and I took the opportunity to untangle it from our own code.


# Maven

```
<dependency>
  <groupId>io.inbot</groupId>
  <artifactId>inbot-es-http-client</artifactId>
  <version>0.9</version>
</dependency>
```

# License

See [LICENSE](LICENSE).

The license is the [MIT license](http://en.wikipedia.org/wiki/MIT_License), a.k.a. the expat license. The rationale for choosing this license is that I want to maximize your freedom to do whatever you want with the code while limiting my liability for the damage this code could potentially do in your hands. I do appreciate attribution but not enough to require it in the license (beyond the obligatory copyright notice).
