_          = require 'lodash'
redis      = require 'fakeredis'
RedisNS    = require '@octoblu/redis-ns'
mongojs    = require 'mongojs'
uuid       = require 'uuid'
Datastore  = require 'meshblu-core-datastore'
JobManager = require 'meshblu-core-job-manager'
EnqueueJobsForSubscriptionsBroadcastReceived = require '../'

describe 'EnqueueJobsForSubscriptionsBroadcastReceived', ->
  beforeEach (done) ->
    @datastore = new Datastore
      database: mongojs 'subscription-test'
      collection: 'subscriptions'
    @datastore.remove done

  beforeEach ->
    @redisKey = uuid.v1()
    @jobManager = new JobManager
      client: new RedisNS 'ns', redis.createClient(@redisKey)
      timeoutSeconds: 1

  beforeEach ->
    client = new RedisNS 'ns', redis.createClient(@redisKey)

    @sut = new EnqueueJobsForSubscriptionsBroadcastReceived {
      datastore: @datastore
      jobManager: new JobManager {client: client, timeoutSeconds: 1}
      uuidAliasResolver: {resolve: (uuid, callback) -> callback(null, uuid)}
    }

  describe '->do', ->
    context 'when there are no subscriptions', ->
      context 'when given a request', ->
        beforeEach (done) ->
          request =
            metadata:
              responseId: 'its-electric'
              fromUuid: 'emitter-uuid'
              toUuid: 'subscriber-uuid'
              options: {}
            rawData: '{}'

          @sut.do request, (error, @response) => done error

        it 'should return a 204', ->
          expectedResponse =
            metadata:
              responseId: 'its-electric'
              code: 204
              status: 'No Content'

          expect(@response).to.deep.equal expectedResponse

    context 'when there is one subscription', ->
      beforeEach (done) ->
        record =
          type: 'broadcast.received'
          emitterUuid: 'subscriber-uuid'
          subscriberUuid: 'some-other-uuid'

        @datastore.insert record, done

      context 'when given a broadcast', ->
        beforeEach (done) ->
          request =
            metadata:
              responseId: 'its-electric'
              fromUuid: 'emitter-uuid'
              toUuid: 'subscriber-uuid'
              options: {}
            rawData: '{"original":"message"}'

          @sut.do request, (error, @response) => done error

        it 'should return a 204', ->
          expectedResponse =
            metadata:
              responseId: 'its-electric'
              code: 204
              status: 'No Content'

          expect(@response).to.deep.equal expectedResponse

        it 'should enqueue a job to deliver the message', (done) ->
          @jobManager.getRequest ['request'], (error, request) =>
            return done error if error?
            delete request?.metadata?.responseId
            expect(request).to.deep.equal {
              metadata:
                jobType: 'DeliverSubscriptionBroadcastReceived'
                auth:
                  uuid: 'some-other-uuid'
                fromUuid: 'subscriber-uuid'
                toUuid: 'some-other-uuid'
                route: [
                 {
                   fromUuid: "emitter-uuid"
                   toUuid: "subscriber-uuid"
                   type: "broadcast.received"
                 }
               ]
              rawData: '{"original":"message"}'
            }
            done()

      context 'when given a message with a previous hop in the route', ->
        beforeEach (done) ->
          request =
            metadata:
              responseId: 'its-electric'
              fromUuid: 'emitter-uuid'
              toUuid: 'subscriber-uuid'
              options: {}
              route: [{
                fromUuid: 'original-uuid'
                toUuid: 'emitter-uuid'
                type: 'broadcast.sent'
              }]
            rawData: '{"original":"message"}'

          @sut.do request, (error, @response) => done error

        it 'should return a 204', ->
          expectedResponse =
            metadata:
              responseId: 'its-electric'
              code: 204
              status: 'No Content'

          expect(@response).to.deep.equal expectedResponse

        it 'should enqueue a job to deliver the broadcast with the hop prepended', (done) ->
          @jobManager.getRequest ['request'], (error, request) =>
            return done error if error?
            delete request?.metadata?.responseId
            expect(request).to.deep.equal {
              metadata:
                jobType: 'DeliverSubscriptionBroadcastReceived'
                auth:
                  uuid: 'some-other-uuid'
                fromUuid: 'subscriber-uuid'
                toUuid: 'some-other-uuid'
                route: [
                  {
                    fromUuid: "emitter-uuid"
                    toUuid: "subscriber-uuid"
                    type: "broadcast.received"
                  },
                  {
                    fromUuid: 'original-uuid'
                    toUuid: 'emitter-uuid'
                    type: 'broadcast.sent'
                  }
                ]
              rawData: '{"original":"message"}'
            }
            done()

      context 'when given a broadcast with a hop in the route equal to this one', ->
        beforeEach (done) ->
          request =
            metadata:
              responseId: 'its-electric'
              fromUuid: 'emitter-uuid'
              toUuid: 'original-uuid'
              options: {}
              route: [{
                fromUuid: 'emitter-uuid'
                toUuid: 'original-uuid'
                type: 'broadcast.received'
              }]
            rawData: '{"original":"message"}'

          @sut.do request, (error, @response) => done error

        it 'should return a 204', ->
          expectedResponse =
            metadata:
              responseId: 'its-electric'
              code: 204
              status: 'No Content'

          expect(@response).to.deep.equal expectedResponse

        it 'should not enqueue a job', (done) ->
          @jobManager.getRequest ['request'], (error, request) =>
            return done error if error?
            expect(request).not.to.exist
            done()
