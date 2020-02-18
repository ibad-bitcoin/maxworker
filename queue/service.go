package queue

func Push(job Queuable) {
	JobQueue <- job
}