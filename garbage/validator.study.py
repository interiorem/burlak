from collections import namedtuple
from cerberus import Validator

import timeit

Some = namedtuple('Some', [
    'hd',
    'tl'
])

print Some(1,100)


schema = {
    'state' : {
        'type' : 'dict',
        'valueschema' : {'type' : 'list', 'items' : [{'type' : 'integer'}, {'type' : 'string'}]},
    },
}

doc = { 'app{}'.format(i): (i, 'Some{}'.format(i)) for i in xrange(1000)}


print 'doc ready'

v = Validator(schema)
print v.validate({'state' : doc})


#if not v.validate('boo'):
#    print 'broken'


schema2 = {
    'state' : {
        'type' : 'list', 
        'schema' : {
            'type': 'list',
            'items': [
                {'type': 'string'},
                {'type': 'integer', 'min' : 0},
            ],
        },
    },
}

doc2 = [('app{}'.format(i), -i) for i in xrange(1000)]


print 'doc2 ready'

v2 = Validator(schema2)
print v2.validate({'state' : doc2})


def is_valid(d):
    for k, (v1, v2) in d.iteritems():
        if not (isinstance(k, str) and isinstance(v1, int) and isinstance(v2, str)):
            return False

    return True  


#print timeit.repeat('v.validate({"state":doc})', setup="from __main__ import doc, v", number=10, repeat=3)
#print timeit.repeat('is_valid(doc)', setup="from __main__ import doc, is_valid", number=10, repeat=3)
