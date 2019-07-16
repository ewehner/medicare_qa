# medicare_qa

A small little app written as a coding exercise. Reads file from a data, parses it according to a schema file, and posts it to a server.

Since data matching between the file and the server was a legal requirement (or to be treated as such) this includes some simulation of a failure rate. This shows how the code will respond if the API responds with a failing status code. The script will retry five times before reporting failure to the users. At this point, one would also connect this to some monitors that would ideally alert a human of the failure.
