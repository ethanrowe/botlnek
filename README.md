# Botlnek - Asynchronous Event Aggregation

Botlnek provides a mechanism for aggregating asynchronous events, with the purpose of simplifying downsteam decision-making.

*Note: This is currently in a exploratory prototype phase, discovering a reasonable interface while deferring matters of persistence and notification for later.*

## Objective

### The initial problem

Consider a relatively common data processing problem:
* Some processing pipeline `A` produces data on some schedule; let's say twice daily.
* Some other processing pipeline `B` produces data on some other schedule; let's say once daily.
* Some third processing pipeline `C` produces some derivative using `A` and `B` as input.
* Let's further claim that we really want `C` to run at least once daily, and it can cope with missing data; it can use a recent `A`, as long as it's within a day or two.  It can use today's `B` or yesterday's `B`.

A typical way to approach this would be to have each of these pipelines on a schedule, to set the `C` pipeline on a schedule where you're reasonably confident the latest `B` and `B` increments will be ready, and leave it at that.  This is an intuitive way to think about the problem.  Pipeline `C` would run on its schedule, search for the best available `A` and `B` increments, blow up if those increments aren't recent enough, and otherwise do the best work it can.

But if your needs for `C` get more complex, or you have a **lot** of these kinds of data dependencies, this approach breaks down.  Schedule reliance indirectly couples pipelines, such that you cannot adjust the schedule of `A` without consideration for `C`.  What if you have pipelines `D`, `E`, and `F`, that also consume either `A` or `B` or both?  Orchestration through scheduling is not a good long term option.

At this point, one might turn to ETL tools that provide some sort of job dependency management.  This can be okay as long as:
* You're happy pushing everyone to a common processing solution, which may be neither feasible nor desirable.
* You're happy organizing your relationships in terms of ETL pipelines, rather than in terms of the availability of data artifacts themselves.

I'm not happy with either of these; our data problems are too varied to naturally fit well within a common framework, which ultimately leads us to emphasize graphs of data artifact dependencies over processing pipeline dependencies.

### Event-driven problem

So instead let's approach this in an event-driven manner.
* Pipeline `A` notifies subscribers of the availability of a new data increment, with some metadata describing said increment.
* Pipeline `B` does the same.
* A subscriber to both of these event sources makes decisions about kicking off pipeline `C` as data becomes available.

Now you have a new problem: you have to implement that pipeline `C` decision maker, and it needs to keep track of information from multiple sources.

It's by no means an insurmountable problem but it's fussy and ceremonial after you solve it a few times.

### Asynchronous event aggregation

This `botlnek` project is an attempt to provide a reasonable generic solution for that subscriber problem.  Very simply:
* Related events collect within a `botlnek` backend as `sources` within a given `aggregate`.
* As new sources come in, `botlnek` maintains a version-aware representation of the `aggregate`; the version history is built into the data structure.
* Each "version" has a sequence number and an approximate timestamp.
* The structure is (generally speaking) append-only, and thus makes a simple guarantee: an aggregate version sequence number increase corresponds to an increase in information known to the aggregate.

The aggregate doesn't do anything; botlnek doesn't do anything beyond maintain this aggregate and send messages about it.  And that's exactly the point.

In our scenario above:
* A subscriber gets pipeline `A` notifications; the subscriber is specific to the relationship between pipeline `A` artifacts and pipeline `C`'s processing needs
    * The subscriber maps the `A` notification to a "aggregate key"; a user-determined natural key specific to a particular botlnek aggregate.  The datestamp might be a reasonable choice.
    * The subscriber registers the pertinent `A` notification data as a "source" within the aggregate identified by the above aggregate key; the source is associate with a "collection key" used for grouping like sources together within the aggregate.  Suppose the collection key is "A".
* A similar subscriber gets pipeline `B` notifications; it also registers the notification with botlnek, for the relevant artifact key, and in this case using a collection key of "B".
* As each source comes in for today's aggregate, botlnek emits a message showing the state of the aggregate.
    * Perhaps initially, that state shows a single version, with a single pipeline-A message as its sole "source", under collection key "A".
    * Perhaps the pipeline-B message came next, which means we get a second botlnek message about the aggregate showing the original pipeline-A message under key "A" and the pipeline-B message under key "B".
    * Finally, the second pipeline-A message comes in (remember that we said pipeline A runs twice daily); we get a third botnek message for the aggregate showing *both* pipeline-A messages under key "A" and the one pipeline-B message under key "B".

We in turn can have a subscriber to the botnek notifications; that subscriber receives these messages about our aggregate of interest.
* It could decide to run pipeline C every time new information is available, which is what you might do if you want to be supremely confident that a C artifact will be available when it's needed.
* It could decide to run pipeline C as soon as both an A and a B input are available, but the first time only (such that it would act on the second message only and ignore the others)
* Or whatever else.

The point is that interrelationships between pipelines are pushed out to the subscribers that act as a translation layer from the domain of the publishing pipeline to the domain of the downstream dependency.  Furthermore, the decision to act or not on a new combination of available inputs is pushed to a subscriber specific to that purpose.

Botlnek, by approaching the aggregate in an append-only manner, gives simple guarantees around the order of version sequence numbers and puts all the information you need for decision-making purposes in one place.

The result is a system with looser coupling, that imposes no opinions on the implementation of the pipelines themselves (by not forcing everything into a common ETL/orchestration framework).

The user of botlnek tailors their use of aggregates to the needs of the downstream consumer for which the aggregate is necessary.  But it's ultimately just reasoning about the different cases of that downstream consumer, which one really needs to understand in any case.

# Well...

That's the idea, anyway.  This is just an in-memory prototype, the data model of which I'm going to update to be more consistent with what I've described above.

