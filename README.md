## Use redis store job status, and the data is only used to monitor the job, Use amqp protocol to route/persistant message
1. On queue setup, it will subscribe the complete/error/progress queue
2. When saving a job, it will be published to "main" queue, and also save to redis with inactive status, 
3. During the process of the job, redis job status will be updated to active status
4. If a job is complete, it will be published to the complete queue, which cause the invoke of on "complete" handler
5. After complete handler invoked, the job is done
6. If a job failed, it will be published to the error queue, and trigger the on "error" handler
7. Error handler will update job status on redis
8. Progress will be both saved to redis and published to progress queue, the on "progress" handler will be triggered then.
