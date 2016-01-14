# Introduction

Java Elasicsearch client that
- uses http REST api and does not require using embedded elasticsearch nodes
- provides useful abstractions for commonly used things in the Elasticsearch API
- provides robustness and proper error handling

# Why and how

At Inbot we have been early adopters of Elasticsearch and we have used it for several years already. While we have Java as part of our architecture, we've always refrained from using the native java API that comes with elasticsearch for several reasons:
- it complicates deployment since now all your backend servers need to have elasticsearch nodes embedded and it prevents proper isolation of these two things
- most of the documentation assumes you use http
- the performance benefits are there but not as dramatic as some would have you believe

We used HTTP from day one and started out with some simple code around apache http client that over the years grew into a mini framework with loads of features. I always had the plan to open source this but never got around to cleaning it up. Recently the release of Elasticsearch 2.0 forced me to finally refactor the code and I took the opportunity to untangle it from our own code.


- no checked exceptions
- avoid boilerplate code at all cost
- deal with raw json via my jsonj framework instead of trying to map each dark corner of the elasticsearch API to some model class
- production quality setup that is robust, has error handling, retries, and can run for years while not running out of file handles.
- utilize Java 8 features such as lambda's, varargs, etc.
- be easy to use and setup
- access elasticsearch through daos that encapsulate all the important interaction with Elasticsearch
 
# Maven

```
<dependency>
  <groupId>io.inbot</groupId>
  <artifactId>inbot-es-http-client</artifactId>
  <version>1.0</version>
</dependency>
```

# License

See [LICENSE](LICENSE).

The license is the [MIT license](http://en.wikipedia.org/wiki/MIT_License), a.k.a. the expat license. The rationale for choosing this license is that I want to maximize your freedom to do whatever you want with the code while limiting my liability for the damage this code could potentially do in your hands. I do appreciate attribution but not enough to require it in the license (beyond the obligatory copyright notice).
