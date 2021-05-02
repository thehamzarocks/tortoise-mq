#!/usr/bin/env python
import sys
from tortoise_mq import tmq_produce 

# there is either a default message or the one entered on the command line
message = ' '.join(sys.argv[1:]) or 'Default Message'

tmq_produce(message)
print(" [x] Sent %r" % message)


