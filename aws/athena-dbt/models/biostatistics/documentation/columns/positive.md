{% docs positive %}

Whether the test returned positive.  This we compute off the raw `result` field, 
which doesn't have an enforced schema, so it's not perfect. We also don't currently 
try to distinguish invalid results from negative results.

{% enddocs %}