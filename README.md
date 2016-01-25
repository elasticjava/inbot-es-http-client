# Introduction

**Work in progress** I'm currently untangling code and tests related to this codebase from our internal codebase. This is an ongoing project. The short term goal is to present this stuff at the Elasticsearch meetup end of January 2016. Beware, API stability is not a goal until we hit 1.0. There will be changes.

# Features

- Json http rest client (with get/put/post/delete support). Takes and returns jsonj's `JsonObject` instances. This client is mainly used as our 'lowest' level of abstraction.
- Rich EsApiClient that uses the http rest client. Loads of convenient methods to deal with document CRUD, searches, etc.
- CrudOperations and ParentChildCrudOperations to provide a convenient way to create DAOs for index and type combinations with or without parent child relations.
- BulkIndexer with support for inserts/deletes/updates, parallelism, error handling, and a call back API.
- Support for paging and scrolling searches
- Redis and Guava caching support with invalidation
- Safe updates that retry their lambda update function in case of a version conflict. Works in bulk as well
- Simple migration helper that you can use to manage schema changes. Note. this currently should not be considered safe in any environment where documents get updated during the migration.
- ...

# Why and how?

At Inbot we have been early adopters of Elasticsearch and we have used it for several years already. While we have Java as part of our architecture, we've always refrained from using the native java API that comes with elasticsearch for several reasons:

- it complicates deployment since now all your backend servers need to have elasticsearch nodes embedded and it prevents proper isolation of these two things
- most of the documentation assumes you use http
- the performance benefits are there but not as dramatic as some would have you believe. This is the same reason that e.g. Logstash no longer ships with embeded elasticsearch as a default option. If you use http correctly, it is mostly the same amount of stuff going over the wire. Our library does the right thing by not needlessly buffering requests/responses, by pooling connections using httpclient, and by supporting gzip compression (if you set up http client to do this).

So, we used HTTP to talk to Elasticsearch from day one and started out with some simple code around apache http client that over the years grew into a mini framework with loads of features. I always had the plan to open source this but never got around to cleaning it up. Recently the release of Elasticsearch 2.0 forced me to finally refactor the code and I took the opportunity to untangle it from our own code.

# State of development

Facts:
- We have been using this in Inbot, it serves our needs and performs & scales well. Aside from recent OSS related refactoring we have had little or no need for fixing anything in these classes.
- We know it works because our internal integration tests use this a lot but test coverage inside this recently ossed project is obviously less than ideal. This needs to be remedied. We welcome pullrequests for this.
- It does not currently support the full ES API. We only implemented the bits we use. The good news is that this is easily addressed. We welcome pull requests for this.
- Nobody else currently is using this because we just released it; so you'll have to take our word for all this ;-)

# Maven

```
<dependency>
  <groupId>io.inbot</groupId>
  <artifactId>inbot-es-http-client</artifactId>
  <version>0.10</version>
</dependency>
```

I've marked several of the dependencies as optional to avoid locking you into specific things as much as possible. In general go with the versions we use, or newer. If stuff doesn't work with a newer version than we use, that is a bug and please report it.


# License

See [LICENSE](LICENSE).

The license is the [MIT license](http://en.wikipedia.org/wiki/MIT_License), a.k.a. the expat license. The rationale for choosing this license is that I want to maximize your freedom to do whatever you want with the code while limiting my liability for the damage this code could potentially do in your hands. I do appreciate attribution but not enough to require it in the license (beyond the obligatory copyright notice).

# Contributing

We welcome pullrequests but encourage people to reach out before making major changes. In general, stick to the code conventions (i.e. maven formatter plugin should not create diffs), don't break compatibility (we use this stuff in production), add tests for new stuff, etc.

# Changes

- 0.10 cleanup, new functinoality, refactoring, tests added.
- 0.9 initial import of classes from inbot code base
