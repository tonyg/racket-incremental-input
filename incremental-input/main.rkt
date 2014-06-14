#lang racket/base

(provide (struct-out suspended-read)
	 incremental-input?
	 (rename-out [create-incremental-input incremental-input])
	 extend-incremental-input!
	 close-incremental-input!
	 start-incremental-read
	 continue-incremental-read)

(require racket/match)
(require data/queue)

(struct suspended-read (k))

(struct incremental-input (ports ch wrapper [thread #:mutable]))

(define (create-incremental-input . initial-ports)
  (define ports (make-queue))
  (define ch (make-channel))
  (define wrapper (make-wrapper ports ch))
  (for [(initial-port (in-list initial-ports))] (enqueue! ports initial-port))
  (incremental-input ports ch wrapper #f))

(define (extend-incremental-input! p . more-ports)
  (define ports (incremental-input-ports p))
  (for [(port (in-list more-ports))] (enqueue! ports port)))

(define (close-incremental-input! p)
  (enqueue! (incremental-input-ports p) #f))

(define (start-incremental-read p read-proc
				#:on-result [on-result values]
				#:on-suspension [on-suspension values])
  (define ch (incremental-input-ch p))
  (when (incremental-input-thread p)
    (error 'start-incremental-read "Incremental read already in progress"))
  (define this-thread (thread (lambda ()
				(define result (read-proc (incremental-input-wrapper p)))
				(channel-put ch (cons 'finished result)))))
  (set-incremental-input-thread! p this-thread)
  (let wait-for-signal ()
    (match (channel-get ch)
      [(cons 'finished result)
       (set-incremental-input-thread! p #f)
       (on-result result)]
      [(cons 'suspended #f)
       (on-suspension (suspended-read 
		       (lambda ()
			 (unless (eq? (incremental-input-thread p) this-thread)
			   (error 'continue-incremental-read "Cannot continue completed read"))
			 (channel-put ch 'continue)
			 (wait-for-signal))))])))

(define (continue-incremental-read suspension)
  ((suspended-read-k suspension)))

;; This should exist in data/queue
(define (queue-peek q)
  (define v (dequeue! q))
  (enqueue-front! q v)
  v)

(define (make-wrapper ports ch)
  (define (suspend)
    (channel-put ch (cons 'suspended #f))
    (match (channel-get ch) ['continue (void)]))

  (define (incremental-read-bytes! buf)
    (let retry ()
      (cond
       [(queue-empty? ports)
	(suspend)
	(retry)]
       [(queue-peek ports)
	=> (lambda (underlying)
	     (match (read-bytes-avail! buf underlying)
	       [(? eof-object?)
		(dequeue! ports)
		(retry)]
	       [(? number? n)
		n]))]
       [else eof])))

  (define (incremental-close)
    (enqueue-front! ports #f))

  ;; (local-require racket/trace)
  ;; (trace incremental-read-bytes! suspend incremental-close)

  (make-input-port 'incremental-input-port
		   incremental-read-bytes!
		   #f
		   incremental-close))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(module+ test
  (require rackunit)
  (require json)

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; Simple tests

  (let ()
    (define p (create-incremental-input))
    (extend-incremental-input! p (open-input-bytes #"12"))
    (check-equal? (start-incremental-read p read-char) #\1)
    (check-equal? (start-incremental-read p read-char) #\2)
    (let ((susp (start-incremental-read p read-char)))
      (check-true (suspended-read? susp))
      (extend-incremental-input! p (open-input-bytes #"34"))
      (check-equal? (continue-incremental-read susp) #\3))
    (check-equal? (start-incremental-read p read-char) #\4)
    (let ((susp (start-incremental-read p read-char)))
      (check-true (suspended-read? susp))
      (close-incremental-input! p)
      (check-equal? (continue-incremental-read susp) eof)
      (check-exn #px"Cannot continue completed read"
		 (lambda () (continue-incremental-read susp)))))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; Line-reading tests

  (let ()
    (define p (create-incremental-input))
    (extend-incremental-input! p (open-input-bytes #"aa\nbb"))
    (check-equal? (start-incremental-read p read-line) "aa")
    (let ((susp (start-incremental-read p read-line)))
      (check-true (suspended-read? susp))
      (extend-incremental-input! p (open-input-bytes #"\ncc"))
      (check-equal? (continue-incremental-read susp) "bb"))
    (let ((susp (start-incremental-read p read-line)))
      (check-true (suspended-read? susp))
      (close-incremental-input! p)
      (check-equal? (continue-incremental-read susp) "cc")
      (check-equal? (start-incremental-read p read-line) eof)))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; JSON-parsing tests

  (let ()
    (define p (create-incremental-input))
    (extend-incremental-input! p (open-input-bytes #"[1,2,"))
    (let ((susp (start-incremental-read p read-json)))
      (check-true (suspended-read? susp))
      (extend-incremental-input! p (open-input-bytes #"3,4]           [5,6\n"))
      ;;                                                   ^^^^^^^^^^^
      ;; This is aggravating. regexp.c seems to take at LEAST 16 bytes
      ;; of lookahead, no matter what regexp is being applied; and
      ;; json uses regexp-try-match to detect end-of-list.
      ;;
      ;; If we don't add gratuitous space here, then even though a
      ;; complete term is available by the time the ']' has been seen,
      ;; regexp-try-match won't recognise it, and so we will get a
      ;; second suspension from the next call to
      ;; continue-incremental-read.
      (check-equal? (continue-incremental-read susp) (list 1 2 3 4)))
    (let ((susp (start-incremental-read p read-json)))
      (check-true (suspended-read? susp))
      (extend-incremental-input! p (open-input-bytes #"]               "))
      (check-equal? (continue-incremental-read susp) (list 5 6)))
    (let ((susp (start-incremental-read p read-json)))
      (check-true (suspended-read? susp))
      (close-incremental-input! p)
      (check-equal? (continue-incremental-read susp) eof))))
