## Summary
<!--
When using the average function over at least one large negative and positive integers it returns 0 instead of the actual average. As both numbers are within the integer limit and valid inputs. As also the summation of these two values is within the integer limits and then diving by the amount of values should give 0.5.
-->
## Minimized query
``` sql
CREATE TABLE t0 (c1 INT);
INSERT INTO t0 (c1) VALUES (9223372036854775807), (-9223372036854775808);
SELECT AVG(c1) FROM t0;
```
## Actual output
```sql
0
```
## Expectation
```sql
-0.5
```