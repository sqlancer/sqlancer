

# TODOS
* SQLancer currently runs also embedded DBMS in the process of the JVM. If SQLancer triggers a crash in the tested DBMS, the JVM also crashes along. Having individiual testing process would address this.

# Test Suite

SQLancer does not have a test suite. We found that bugs in SQLancer are quickly found when testing the DBMS. The PQS implementation had a test suite, which was removed in commit 36ede0c0c68b3856e03ef5ba802a7c2575bb3f12.
