<h2 id="jsonpath-concepts">JSONPath Concepts</h2>
<p>When a state sets the query language to JSONPath, the interpreter
supports fields and syntax for querying data using JSONPath paths.
For information about JSONata, see
<a href="#jsonata-concepts">JSONata Concepts</a>.</p>
When a state sets the query language to JSONPath, the interpreter
supports fields and syntax for querying data using JSONPath paths.
For information about JSONata, see

<a href="#jsonata-concepts">JSONata Concepts</a>
.
<p>When a state sets the query language to JSONPath, the interpreter
supports fields and syntax for querying data using JSONPath paths.
For information about JSONata, see
<a href="#jsonata-concepts">JSONata Concepts</a>.</p>
<h3 id="path">Paths</h3>
<p>A Path is a string, beginning with "$", used to identify components
within a JSON text. The syntax is that of
<a href="https://github.com/jayway/JsonPath" title="JSONPath" rel="noopener noreferrer" target="_blank">JSONPath</a>.</p>
<p>When a Path begins with "$$", two dollar signs, this signals that it
is intended to identify content within the Context Object. The first
dollar sign is stripped, and the remaining text, which begins with a
dollar sign, is interpreted as the JSONPath applying to the Context
Object.</p>
<h3 id="ref-paths">Reference Paths</h3>
<p>A Reference Path is a Path with syntax limited in such a way that it can
only identify a single node in a JSON structure: the operators "@", ",",
":", and "?" are not supported - all Reference Paths MUST be unambiguous
references to a single value, array, or object (subtree).</p>
<p>For example, if state input data contained the values:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"foo"</span><span class="p">:</span> <span class="mi">123</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"bar"</span><span class="p">:</span> <span class="p">[</span><span class="s2">"a"</span><span class="p">,</span> <span class="s2">"b"</span><span class="p">,</span> <span class="s2">"c"</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"car"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">      <span class="nt">"cdr"</span><span class="p">:</span> <span class="kc">true</span>
</span></span><span class="line"><span class="cl">  <span class="p">}</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>Then the following Reference Paths would return:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-text" data-lang="text"><span class="line"><span class="cl">$.foo =&gt; 123
</span></span><span class="line"><span class="cl">$.bar =&gt; ["a", "b", "c"]
</span></span><span class="line"><span class="cl">$.car.cdr =&gt; true
</span></span></code></pre></div>
<p>Paths and Reference Paths are used by certain states, as specified later
in this document, to control the flow of a state machine or to configure
a state's settings or options.</p>
<p>Here are some examples of acceptable Reference Path syntax:</p>
<pre tabindex="0"><code class="language-nocheck" data-lang="nocheck">$.store.book
$.store\.book
$.\stor\e.boo\k
$.store.book.title
$.foo.\.bar
$.foo\@bar.baz\[\[.\?pretty
$.&amp;Ж中.\uD800\uDF46
$.ledgers.branch[0].pending.count
$.ledgers.branch[0]
$.ledgers[0][22][315].foo
$['store']['book']
$['store'][0]['book']
</code></pre>
<h3 id="payload-template">Payload Template</h3>
<p>The interpreter dispatches data as input to tasks to do
useful work, and receives output back from them. A common requirement
is to reshape input data to meet the format expectations of tasks,
and similarly to reshape the output coming back. A JSON object structure
called a Payload Template is provided for this purpose.</p>
<p>In the Task, Map, Parallel, and Pass States, the Payload Template is the
value of a field named "Parameters". In the Task, Map, and Parallel
States, there is another Payload Template which is the value of a field
named "ResultSelector".</p>
<p>A Payload Template MUST be a JSON object; it has no required fields. The
interpreter processes the Payload Template as described in this section;
the result of that processing is called the payload.</p>
<p>To illustrate by example, the Task State has a field named "Parameters"
whose value is a Payload Template. Consider the following Task State:</p>
<pre tabindex="0"><code class="language-state" data-lang="state">"X": {
  "Type": "Task",
  "Resource": "arn:aws:states:us-east-1:123456789012:task:X",
  "Next": "Y",
  "Parameters": {
    "first": 88,
    "second": 99
  }
}
</code></pre>
<p>In this case, the payload is the object with "first" and "second" fields
whose values are respectively 88 and 99. No processing needs to be
performed and the payload is identical to the Payload Template.</p>
<p>Values from the Payload Template’s input and the Context Object can be
inserted into the payload with a combination of a field-naming
convention, Paths and Intrinsic Functions.</p>
<p>If any field within the Payload Template (however deeply nested) has a
name ending with the characters ".$", its value is transformed
according to the following rules and the field is renamed to strip the ".$"
suffix.</p>
<p>If the field value begins with only one "$", the value MUST be a Path.
In this case, the Path is applied to the Payload Template’s input and is
the new field value.</p>
<p>If the field value begins with "$$", the first dollar sign is stripped
and the remainder MUST be a Path. In this case, the Path is applied to
the Context Object and is the new field value.</p>
<p>If the field value does not begin with "$", it MUST be an Intrinsic
Function (<a href="#intrinsic-functions">see Intrinsic Functions</a>). The interpreter invokes the Intrinsic Function and
the result is the new field value.</p>
<p>If the path is legal but cannot be applied successfully, the interpreter
fails the machine execution with an Error Name of
"States.ParameterPathFailure". If the Intrinsic Function fails during
evaluation, the interpreter fails the machine execution with an Error
Name of "States.IntrinsicFailure".</p>
<p>A JSON object MUST NOT have duplicate field names after fields ending
with the characters ".$" are renamed to strip the ".$" suffix.</p>
<pre tabindex="0"><code class="language-state" data-lang="state">"X": {
  "Type": "Task",
  "Resource": "arn:aws:states:us-east-1:123456789012:task:X",
  "Next": "Y",
  "Parameters": {
    "flagged": true,
    "parts": {
      "first.$": "$.vals[0]",
      "last3.$": "$.vals[-3:]"
    },
    "weekday.$": "$$.DayOfWeek",
    "formattedOutput.$": "States.Format('Today is {}', $$.DayOfWeek)"
  }
}
</code></pre>
<p>Suppose that the input to the P is as follows:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"flagged"</span><span class="p">:</span> <span class="mi">7</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"vals"</span><span class="p">:</span> <span class="p">[</span><span class="mi">0</span><span class="p">,</span> <span class="mi">10</span><span class="p">,</span> <span class="mi">20</span><span class="p">,</span> <span class="mi">30</span><span class="p">,</span> <span class="mi">40</span><span class="p">,</span> <span class="mi">50</span><span class="p">]</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>Further, suppose that the Context Object is as follows:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"DayOfWeek"</span><span class="p">:</span> <span class="s2">"TUESDAY"</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>In this case, the effective input to the code identified in the
"Resource" field would be as follows:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"flagged"</span><span class="p">:</span> <span class="kc">true</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"parts"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"first"</span><span class="p">:</span> <span class="mi">0</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"last3"</span><span class="p">:</span> <span class="p">[</span><span class="mi">30</span><span class="p">,</span> <span class="mi">40</span><span class="p">,</span> <span class="mi">50</span><span class="p">]</span>
</span></span><span class="line"><span class="cl">  <span class="p">},</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"weekday"</span><span class="p">:</span> <span class="s2">"TUESDAY"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"formattedOutput"</span><span class="p">:</span> <span class="s2">"Today is TUESDAY"</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<h3 id="intrinsic-functions">Intrinsic Functions</h3>
<p>The States Language provides a small number of "Intrinsic Functions", constructs
which look like functions in programming languages and can be used to help
Payload Templates process the data going to and from Task Resources. See
<a href="#appendix-b">Appendix B</a> for a full list of
Intrinsic Functions</p>
<p>Here is an example of an Intrinsic Function named "States.Format"
being used to prepare data:</p>
<pre tabindex="0"><code class="language-state" data-lang="state">"X": {
  "Type": "Task",
  "Resource": "arn:aws:states:us-east-1:123456789012:task:X",
  "Next": "Y",
  "Parameters": {
    "greeting.$": "States.Format('Welcome to {} {}\\'s playlist.', $.firstName, $.lastName)"
  }
}
</code></pre>
<ol>
<li>
<p>An Intrinsic Function MUST be a string.</p>
</li>
<li>
<p>The Intrinsic Function MUST begin with an Intrinsic Function name.
An Intrinsic Function name MUST contain only the characters A
through Z, a through z, 0 through 9, ".", and "_".</p>
<p>All Intrinsic Functions defined by this specification have names
that begin with "States.". The interpreter MAY define its
own Intrinsic Functions whose names MUST NOT begin with "States.".</p>
</li>
<li>
<p>The Intrinsic Function name MUST be followed immediately by a list
of zero or more arguments, enclosed by "(" and ")", and separated by
commas.</p>
</li>
<li>
<p>Intrinsic Function arguments may be strings enclosed by apostrophe
(<code>'</code>) characters, numbers, null, Paths, or nested Intrinsic
Functions.</p>
</li>
<li>
<p>The value of a string, number or null argument is the argument
itself. The value of an argument which is a Path is the result of
applying it to the input of the Payload Template. The value of an
argument which is an Intrinsic Function is the result of the
function invocation."</p>
<p>Note that in the previous example, the first argument of
<code>States.Format</code> could have been a Path that yielded the formatting
template string.</p>
</li>
<li>
<p>The following characters are reserved for all Intrinsic Functions
and MUST be escaped: ' { } \</p>
<p>If any of the reserved characters needs to appear as part of the
value without serving as a reserved character, it MUST be escaped
with a backslash.</p>
<p>If the character "\ needs to appear as part of the value without
serving as an escape character, it MUST be escaped with a backslash.</p>
<p>The literal string <code>\'</code> represents <code>'</code>.<br>
The literal string <code>\{</code> represents <code>{</code>.<br>
The literal string <code>\}</code> represents <code>}</code>.<br>
The literal string <code>\\</code> represents <code>\</code>.</p>
<p>In JSON, all backslashes contained in a string literal value must be
escaped with another backslash, therefore, the preceding sequences will equate to:</p>
<p>The escaped string <code>\\'</code> represents <code>'</code>.<br>
The escaped string <code>\\{</code> represents <code>{</code>.<br>
The escaped string <code>\\}</code> represents <code>}</code>.<br>
The escaped string <code>\\\\</code> represents <code>\</code>.</p>
<p>If an open escape backslash <code>\</code> is found in the Intrinsic Function,
the interpreter will throw a runtime error.</p>
</li>
</ol>
<h3 id="reserved-states-variable-with-jsonpath">Reserved "states" Variable with JSONPath</h3>
<p>The States Language reserves one variable called "states".  When the
query language is JSONPath, its value is defined by the interpreter.
This version of the States Language specification does not specify any
contents of the "states" variable when the query language is JSONPath.
A state MUST NOT assign a value to "states".</p>
<h3 id="filters">Input and Output Processing with JSONPath</h3>
<p>As described earlier, data is passed between states as JSON texts.
However, a state may need to process only a subset of its input data,
may need to include a portion of one or more variable values,
and may need that data structured differently from the way it appears
in the input or variables.  Similarly, it may need to control the
format and content of the data that it passes on as output or that it
assigns to variables.</p>
<p>Fields named "InputPath", "Parameters", "ResultSelector", "ResultPath",
and "OutputPath" exist to support this.</p>
<p>Any state except for Succeed and Fail MAY have "InputPath"
and "OutputPath".</p>
<p>States which potentially generate results MAY have "Parameters",
"ResultSelector" and "ResultPath": Task State, Parallel State, and Map
State.</p>
<p>Pass State MAY have "Parameters" and "ResultPath" to control its output
value.</p>
<h4 id="using-inputpath-parameters-resultselector-resultpath-and-outputpath">Using InputPath, Parameters, ResultSelector, ResultPath and OutputPath</h4>
<p>In this discussion, "raw input" means the JSON text that is the input to
a state. "Result" means the JSON text that a state generates, for
example from external code invoked by a Task State, the combined result
of the branches in a Parallel or Map State, or the Value of the "Result"
field in a Pass State. "Effective input" means the input after the
application of InputPath and Parameters, "effective result" means the
result after processing it with ResultSelector and "effective output"
means the final state output after processing the result with
ResultSelector, ResultPath and OutputPath.</p>
<ol>
<li>
<p>The value of "InputPath" MUST be a Path, which is applied to a
State’s raw input to select some or all of it; that selection is
used by the state, for example in passing to Resources in Task
States and Choices selectors in Choice States.</p>
</li>
<li>
<p>The value of "Parameters" MUST be a Payload Template which is a JSON
object, whose input is the result of applying the InputPath to the
raw input. If the "Parameters" field is provided, its payload, after
the extraction and embedding, becomes the effective input.</p>
</li>
<li>
<p>The value of "ResultSelector" MUST be a Payload Template, whose
input is the result, and whose payload replaces and becomes the
effective result.</p>
</li>
<li>
<p>The value of "ResultPath" MUST be a Reference Path, which specifies
the raw input’s combination with or replacement by the state’s
result.</p>
<p>The value of "ResultPath" MUST NOT begin with "$$"; i.e. it may
not be used to insert content into the Context Object.</p>
</li>
<li>
<p>The value of "OutputPath" MUST be a Path, which is applied to the
state’s output after the application of ResultPath, producing the
effective output which serves as the raw input for the next state.</p>
</li>
</ol>
<p>Note that JSONPath can yield multiple values when applied to an input
JSON text. For example, given the text:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"a"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span> <span class="mi">2</span><span class="p">,</span> <span class="mi">3</span><span class="p">,</span> <span class="mi">4</span><span class="p">]</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Then if the JSONPath <code>$.a[0,1]</code> is applied, the result will be two JSON
texts, <code>1</code> and <code>2</code>. When this happens, to produce the effective input,
the interpreter gathers the texts into an array, so in this example the
state would see the input:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">[</span> <span class="mi">1</span><span class="p">,</span> <span class="mi">2</span> <span class="p">]</span>
</span></span></code></pre></div>
<p>The same rule applies to OutputPath processing; if the OutputPath result
contains multiple values, the effective output is a JSON array
containing all of them.</p>
<p>The "ResultPath" field’s value is a Reference Path that specifies where
to place the result, relative to the raw input. If the raw input has a
field at the location addressed by the ResultPath value then in the
output that field is discarded and overwritten by the state's result.
Otherwise, a new field is created in the state output, with intervening
fields constructed as necessary. For example, given the raw input:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"master"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"detail"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span> <span class="mi">2</span><span class="p">,</span> <span class="mi">3</span><span class="p">]</span>
</span></span><span class="line"><span class="cl">  <span class="p">}</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"master"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"detail"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span> <span class="mi">2</span><span class="p">,</span> <span class="mi">3</span><span class="p">]</span>
</span></span><span class="line"><span class="cl">  <span class="p">}</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"master"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"detail"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span> <span class="mi">2</span><span class="p">,</span> <span class="mi">3</span><span class="p">]</span>
</span></span><span class="line"><span class="cl">  <span class="p">}</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>If the state's result is the number <code>6</code>, and the "ResultPath" is
<code>$.master.detail</code>, then in the output the <code>detail</code> field would be
overwritten:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"master"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"detail"</span><span class="p">:</span> <span class="mi">6</span>
</span></span><span class="line"><span class="cl">  <span class="p">}</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>If instead a "ResultPath" of <code>$.master.result.sum</code> was used then the
result would be combined with the raw input, producing a chain of new
fields containing <code>result</code> and <code>sum</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"master"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"detail"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span> <span class="mi">2</span><span class="p">,</span> <span class="mi">3</span><span class="p">],</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"result"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">      <span class="nt">"sum"</span><span class="p">:</span> <span class="mi">6</span>
</span></span><span class="line"><span class="cl">    <span class="p">}</span>
</span></span><span class="line"><span class="cl">  <span class="p">}</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>If the value of InputPath is <code>null</code>, that means that the raw input is
discarded, and the effective input for the state is an empty JSON
object, <code>{}</code>. Note that having a value of <code>null</code> is different from the
"InputPath" field being absent.</p>
<p>If the value of ResultPath is <code>null</code>, that means that the state’s result
is discarded and its raw input becomes its result.</p>
<p>If the value of OutputPath is <code>null</code>, that means the input and result
are discarded, and the effective output from the state is an empty JSON
object, <code>{}</code>.</p>
<h4 id="using-assign-1">Using Assign</h4>
<p>The value of "$" in "Assign" depends on the state type. In Task, Map,
and Parallel States, "$" refers to the result in a state's top-level "Assign",
and to the Error Output in a Catcher's "Assign".
In Choice and Wait States, "$" refers to the effective
input, which is the value after "InputPath" has been applied to the
state input. For Pass State, "$" refers to the result, whether
generated by the "Result" field or the "InputPath" and "Parameters"
fields.</p>
<h4 id="defaults">Defaults</h4>
<p>Each of InputPath, Parameters, ResultSelector, ResultPath, and
OutputPath are optional. The default value of InputPath is "$", so by
default the effective input is just the raw input. The default value of
ResultPath is "$", so by default a state’s result overwrites and
replaces the input. The default value of OutputPath is "$", so by
default a state’s effective output is the result of processing
ResultPath.</p>
<p>Parameters and ResultSelector have no default value. If absent, they
have no effect on their input.</p>
<p>Therefore, if none of InputPath, Parameters, ResultSelector, ResultPath,
or OutputPath are supplied, a state consumes the raw input as provided
and passes its result to the next state.</p>
<h4 id="inputoutput-processing-examples">Input/Output Processing Examples</h4>
<p>Consider the previous example of a Lambda task that sums a pair of
numbers. As presented, its input is: <code>{ "val1": 3, "val2": 4 }</code> and its
output is: <code>7</code>.</p>
<p>Suppose the input is little more complex:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"title"</span><span class="p">:</span> <span class="s2">"Numbers to add"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"numbers"</span><span class="p">:</span> <span class="p">{</span> <span class="nt">"val1"</span><span class="p">:</span> <span class="mi">3</span><span class="p">,</span> <span class="nt">"val2"</span><span class="p">:</span> <span class="mi">4</span> <span class="p">}</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>Then suppose we modify the state definition by adding:</p>
<pre tabindex="0"><code class="language-nocheck" data-lang="nocheck">"InputPath": "$.numbers",
"ResultPath": "$.sum"
</code></pre>
<p>And finally, suppose we simplify Line 4 of the Lambda function to read as
follows: <code>return&nbsp;JSON.stringify(total)</code>. This is probably a better form
of the function, which should really only care about doing math and not
care how its result is labeled.</p>
<p>In this case, the output would be:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"title"</span><span class="p">:</span> <span class="s2">"Numbers to add"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"numbers"</span><span class="p">:</span> <span class="p">{</span> <span class="nt">"val1"</span><span class="p">:</span> <span class="mi">3</span><span class="p">,</span> <span class="nt">"val2"</span><span class="p">:</span> <span class="mi">4</span> <span class="p">},</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"sum"</span><span class="p">:</span> <span class="mi">7</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>The interpreter might need to construct multiple levels of JSON object
to achieve the desired effect. Suppose the input to some Task State is:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"a"</span><span class="p">:</span> <span class="mi">1</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Suppose the output from the Task is "Hi!", and the value of the
"ResultPath" field is "$.b.greeting". Then the output from the state
would be:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"a"</span><span class="p">:</span> <span class="mi">1</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"b"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"greeting"</span><span class="p">:</span> <span class="s2">"Hi!"</span>
</span></span><span class="line"><span class="cl">  <span class="p">}</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<h4 id="jsonpath-runtime-errors">JSONPath Runtime Errors</h4>
<p>Suppose a state’s input is the string <code>"foo"</code>, and its "ResultPath"
field has the value "$.x". Then ResultPath cannot apply and the
interpreter fails the machine with an Error Name of
"States.ResultPathMatchFailure".</p>

<h2 id="appendices">Appendices</h2>
<h3 id="appendix-a">Appendix A: Predefined Error Codes</h3>
<table>
<thead>
<tr>
<th>Code</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>States.ALL</td>
<td>A wildcard which matches any Error Name.</td>
</tr>
<tr>
<td>States.HeartbeatTimeout</td>
<td>A Task State failed to heartbeat for a time longer than the "HeartbeatSeconds" value.</td>
</tr>
<tr>
<td>States.Timeout</td>
<td>A Task State either ran longer than the "TimeoutSeconds" value, or failed to heartbeat for a time longer than the "HeartbeatSeconds" value.</td>
</tr>
<tr>
<td>States.TaskFailed</td>
<td>A Task State failed during the execution.</td>
</tr>
<tr>
<td>States.Permissions</td>
<td>A Task State failed because it had insufficient privileges to execute the specified code.</td>
</tr>
<tr>
<td>States.ResultPathMatchFailure</td>
<td>A state’s "ResultPath" field cannot be applied to the input the state received.</td>
</tr>
<tr>
<td>States.ParameterPathFailure</td>
<td>Within a state’s "Parameters" field, the attempt to replace a field whose name ends in ".$" using a Path failed.</td>
</tr>
<tr>
<td>States.QueryEvaluationError</td>
<td>Query evaluation failed in a JSONata state, such as a JSONata type error, an incorrectly typed result, or an undefined result.</td>
</tr>
<tr>
<td>States.BranchFailed</td>
<td>A branch of a Parallel State failed.</td>
</tr>
<tr>
<td>States.NoChoiceMatched</td>
<td>A Choice State failed to find a match for the condition field extracted from its input.</td>
</tr>
<tr>
<td>States.IntrinsicFailure</td>
<td>Within a Payload Template, the attempt to invoke an Intrinsic Function failed.</td>
</tr>
<tr>
<td>States.ExceedToleratedFailureThreshold</td>
<td>A Map state failed because the number of failed items exceeded the configured tolerated failure threshold.</td>
</tr>
<tr>
<td>States.ItemReaderFailed</td>
<td>A Map state failed to read all items as specified by the "ItemReader" field.</td>
</tr>
<tr>
<td>States.ResultWriterFailed</td>
<td>A Map state failed to write all results as specified by the "ResultWriter" field.</td>
</tr>
</tbody>
</table>
<h3 id="appendix-b">Appendix B: List of JSONPath Intrinsic Functions</h3>
<p>The States Language provides a set of intrinsic functions that provide
additional functionality for JSONPath.  Intrinsic functions MAY be
used in states that use the JSONPath query language, and MUST NOT be
used in states that use any other query language.</p>
<h4 id="statesformat">States.Format</h4>
<p>This Intrinsic Function takes one or more arguments. The Value of the
first MUST be a string, which MAY include zero or more instances of the
character sequence <code>{}</code>. There MUST be as many remaining arguments in
the Intrinsic Function as there are occurrences of <code>{}</code>. The interpreter
returns the first-argument string with each <code>{}</code> replaced by the Value
of the positionally-corresponding argument in the Intrinsic Function.</p>
<p>If necessary, the <code>{</code> and <code>}</code> characters can be escaped respectively as
<code>\\{</code> and <code>\\}</code>.</p>
<p>If the argument is a Path, applying it to the input MUST yield a value
that is a string, a boolean, a number, or <code>null</code>. In each case, the
Value is the natural string representation; string values are not
accompanied by enclosing <code>"</code> characters. The Value MUST NOT be a JSON
array or object.</p>
<p>For example, given the following Payload Template:</p>
<pre tabindex="0"><code class="language-member" data-lang="member">{
  "Parameters": {
    "foo.$": "States.Format('Your name is {}, we are in the year {}', $.name, 2020)"
  }
}
</code></pre>
<p>Suppose the input to the Payload Template is as follows:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"name"</span><span class="p">:</span> <span class="s2">"Foo"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"zebra"</span><span class="p">:</span> <span class="s2">"stripe"</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>After processing the Payload Template, the new payload becomes:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"foo"</span><span class="p">:</span> <span class="s2">"Your name is Foo, we are in the year 2020"</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesstringtojson">States.StringToJson</h4>
<p>This Intrinsic Function takes a single argument whose Value MUST be a
string. The interpreter applies a JSON parser to the Value and returns
its parsed JSON form.</p>
<p>For example, given the following Payload Template:</p>
<pre tabindex="0"><code class="language-member" data-lang="member">{
  "Parameters": {
    "foo.$": "States.StringToJson($.someString)"
  }
}
</code></pre>
<p>Suppose the input to the Payload Template is as follows:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"someString"</span><span class="p">:</span> <span class="s2">"{\"number\": 20}"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"zebra"</span><span class="p">:</span> <span class="s2">"stripe"</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>After processing the Payload Template, the new payload becomes:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"foo"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"number"</span><span class="p">:</span> <span class="mi">20</span>
</span></span><span class="line"><span class="cl">  <span class="p">}</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesjsontostring">States.JsonToString</h4>
<p>This Intrinsic Function takes a single argument which MUST be a Path.
The interpreter returns a string which is a JSON text representing the
data identified by the Path.</p>
<p>For example, given the following Payload Template:</p>
<pre tabindex="0"><code class="language-member" data-lang="member">{
  "Parameters": {
    "foo.$": "States.JsonToString($.someJson)"
  }
}
</code></pre>
<p>Suppose the input to the Payload Template is as follows:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"someJson"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"name"</span><span class="p">:</span> <span class="s2">"Foo"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"year"</span><span class="p">:</span> <span class="mi">2020</span>
</span></span><span class="line"><span class="cl">  <span class="p">},</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"zebra"</span><span class="p">:</span> <span class="s2">"stripe"</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>After processing the Payload Template, the new payload becomes:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"foo"</span><span class="p">:</span> <span class="s2">"{\"name\":\"Foo\",\"year\":2020}"</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesarray">States.Array</h4>
<p>This Intrinsic Function takes zero or more arguments. The interpreter
returns a JSON array containing the Values of the arguments, in the
order provided.</p>
<p>For example, given the following Payload Template:</p>
<pre tabindex="0"><code class="language-member" data-lang="member">{
  "Parameters": {
    "foo.$": "States.Array('Foo', 2020, $.someJson, null)"
  }
}
</code></pre>
<p>Suppose the input to the Payload Template is as follows:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"someJson"</span><span class="p">:</span> <span class="p">{</span>
</span></span><span class="line"><span class="cl">    <span class="nt">"random"</span><span class="p">:</span> <span class="s2">"abcdefg"</span>
</span></span><span class="line"><span class="cl">  <span class="p">},</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"zebra"</span><span class="p">:</span> <span class="s2">"stripe"</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>After processing the Payload Template, the new payload becomes:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"foo"</span><span class="p">:</span> <span class="p">[</span>
</span></span><span class="line"><span class="cl">    <span class="s2">"Foo"</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="mi">2020</span><span class="p">,</span>
</span></span><span class="line"><span class="cl">    <span class="p">{</span>
</span></span><span class="line"><span class="cl">      <span class="nt">"random"</span><span class="p">:</span> <span class="s2">"abcdefg"</span>
</span></span><span class="line"><span class="cl">    <span class="p">},</span>
</span></span><span class="line"><span class="cl">    <span class="kc">null</span>
</span></span><span class="line"><span class="cl">  <span class="p">]</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesarraypartition">States.ArrayPartition</h4>
<p>Use the&nbsp;<code>States.ArrayPartition</code>&nbsp;intrinsic function to partition a large
array or to slice the data and then send
the payload in smaller chunks.</p>
<p>This intrinsic function takes two arguments. The first argument is an
array, while the second argument defines the chunk size. The interpreter
chunks the input array into multiple arrays of the size specified by
chunk size. The length of the last array chunk may be less than the
length of the previous array chunks if the number of remaining items in
the array is smaller than the chunk size.</p>
<p><strong>Input validation</strong></p>
<ul>
<li>
<p>The first argument MUST be an array.</p>
</li>
<li>
<p>The second argument MUST be a non-zero, positive integer,
representing the chunk size value.</p>
</li>
</ul>
<p>For example, given the following input array:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"inputArray"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span><span class="mi">2</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">4</span><span class="p">,</span><span class="mi">5</span><span class="p">,</span><span class="mi">6</span><span class="p">,</span><span class="mi">7</span><span class="p">,</span><span class="mi">8</span><span class="p">,</span><span class="mi">9</span><span class="p">]</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Use the&nbsp;<code>States.ArrayPartition</code>&nbsp;function to divide the array
into chunks of four values:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"inputArray.$"</span><span class="p">:</span> <span class="s2">"States.ArrayPartition($.inputArray,4)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Which would return the following array chunks:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"inputArray"</span><span class="p">:</span> <span class="p">[</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span><span class="mi">2</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">4</span><span class="p">],</span> <span class="p">[</span><span class="mi">5</span><span class="p">,</span><span class="mi">6</span><span class="p">,</span><span class="mi">7</span><span class="p">,</span><span class="mi">8</span><span class="p">],</span> <span class="p">[</span><span class="mi">9</span><span class="p">]]</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>In the previous example, the&nbsp;<code>States.ArrayPartition</code>&nbsp;function outputs
three arrays. The first two arrays each contain four values, as defined
by the chunk size. A third array contains the remaining value and is
smaller than the defined chunk size.</p>
<h4 id="statesarraycontains">States.ArrayContains</h4>
<p>Use the&nbsp;<code>States.ArrayContains</code>&nbsp;intrinsic function to determine if a
specific value is present in an array. For example, use this
function to detect if there was an error in a&nbsp;<code>Map</code>&nbsp;state iteration.</p>
<p>This intrinsic function takes two arguments. The first argument is an
array, while the second argument is the value to be searched for within
the array.</p>
<p><strong>Input validation</strong></p>
<ul>
<li>The first argument MUST be an array</li>
<li>The second argument MUST be a JSON object</li>
</ul>
<p>For example, given the following input array:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"inputArray"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span><span class="mi">2</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">4</span><span class="p">,</span><span class="mi">5</span><span class="p">,</span><span class="mi">6</span><span class="p">,</span><span class="mi">7</span><span class="p">,</span><span class="mi">8</span><span class="p">,</span><span class="mi">9</span><span class="p">],</span> <span class="nt">"lookingFor"</span><span class="p">:</span> <span class="mi">5</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Use the&nbsp;<code>States.ArrayContains</code>&nbsp;function to find
the&nbsp;<code>lookingFor</code>&nbsp;value within the&nbsp;<code>inputArray</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"contains.$"</span><span class="p">:</span> <span class="s2">"States.ArrayContains($.inputArray, $.lookingFor)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Because the value stored in&nbsp;<code>lookingFor</code>&nbsp;is included in
the&nbsp;<code>inputArray</code>,&nbsp;<code>States.ArrayContains</code>&nbsp;returns the following result:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"contains"</span><span class="p">:</span> <span class="kc">true</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesarrayrange">States.ArrayRange</h4>
<p>Use the&nbsp;<code>States.ArrayRange</code>&nbsp;intrinsic function to create a new array
containing a specific range of elements. The new array can contain up to
1000 elements.</p>
<p>This function takes three arguments. The first argument is the first
element of the new array, the second argument is the final element of
the new array, and the third argument is the increment value between the
elements in the new array.</p>
<p><strong>Input validation</strong></p>
<ul>
<li>
<p>All arguments MUST be integer values.</p>
</li>
<li>
<p>The third argument MUST NOT be zero.</p>
</li>
<li>
<p>The newly generated array MUST NOT contain more than 1000 items.</p>
</li>
</ul>
<p>For example, the following use of the&nbsp;<code>States.ArrayRange</code>&nbsp;function will
create an array with a first value of 1, a final value of 9, and values
in between the first and final values increase by two for each item:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"array.$"</span><span class="p">:</span> <span class="s2">"States.ArrayRange(1, 9, 2)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Which would return the following array:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"array"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">5</span><span class="p">,</span><span class="mi">7</span><span class="p">,</span><span class="mi">9</span><span class="p">]</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesarraygetitem">States.ArrayGetItem</h4>
<p>This intrinsic function returns a specified index's value. This function
takes two arguments. The first argument is an array of values and the
second argument is the array index of the value to return.</p>
<p>For example, use the following&nbsp;<code>inputArray</code>&nbsp;and&nbsp;<code>index</code>&nbsp;values:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"inputArray"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span><span class="mi">2</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">4</span><span class="p">,</span><span class="mi">5</span><span class="p">,</span><span class="mi">6</span><span class="p">,</span><span class="mi">7</span><span class="p">,</span><span class="mi">8</span><span class="p">,</span><span class="mi">9</span><span class="p">],</span> <span class="nt">"index"</span><span class="p">:</span> <span class="mi">5</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Use the&nbsp;<code>States.ArrayGetItem</code>&nbsp;function to
return the value in the&nbsp;<code>index</code>&nbsp;position 5 within the array:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"item.$"</span><span class="p">:</span> <span class="s2">"States.ArrayGetItem($.inputArray, $.index)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>In this example,&nbsp;<code>States.ArrayGetItem</code>&nbsp;would return the following
result:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"item"</span><span class="p">:</span> <span class="mi">6</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesarraylength">States.ArrayLength</h4>
<p>The&nbsp;<code>States.ArrayLength</code>&nbsp;intrinsic function returns the length of an
array. It has one argument, the array to return the length of.</p>
<p>For example, given the following input array:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"inputArray"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span><span class="mi">2</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">4</span><span class="p">,</span><span class="mi">5</span><span class="p">,</span><span class="mi">6</span><span class="p">,</span><span class="mi">7</span><span class="p">,</span><span class="mi">8</span><span class="p">,</span><span class="mi">9</span><span class="p">]</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Use&nbsp;<code>States.ArrayLength</code>&nbsp;to return the length of&nbsp;<code>inputArray</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"length.$"</span><span class="p">:</span> <span class="s2">"States.ArrayLength($.inputArray)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>In this example,&nbsp;<code>States.ArrayLength</code>&nbsp;would return the following JSON
object that represents the array length:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"length"</span><span class="p">:</span> <span class="mi">9</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesarrayunique">States.ArrayUnique</h4>
<p>The&nbsp;<code>States.ArrayUnique</code>&nbsp;intrinsic function removes duplicate values
from an array and returns an array containing only unique elements. This
function takes an array, which can be unsorted, as its sole argument.</p>
<p>For example, the following&nbsp;<code>inputArray</code>&nbsp;contains a series of duplicate
values:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"inputArray"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span><span class="mi">2</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">4</span><span class="p">]</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>The&nbsp;<code>States.ArrayUnique</code>&nbsp;function can be used to remove duplicate
values from the array:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"array.$"</span><span class="p">:</span> <span class="s2">"States.ArrayUnique($.inputArray)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>The&nbsp;<code>States.ArrayUnique</code>&nbsp;function would return the following array
containing only unique elements, removing all duplicate values:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"array"</span><span class="p">:</span> <span class="p">[</span><span class="mi">1</span><span class="p">,</span><span class="mi">2</span><span class="p">,</span><span class="mi">3</span><span class="p">,</span><span class="mi">4</span><span class="p">]</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesbase64encode">States.Base64Encode</h4>
<p>Use the&nbsp;<code>States.Base64Encode</code>&nbsp;intrinsic function to encode data based on
MIME Base64 encoding scheme. For example, use this function to pass data to
other AWS services without using an AWS Lambda function.</p>
<p>This function takes a data string of up to 10,000 characters to encode
as its only argument.</p>
<p>For example, consider the following&nbsp;<code>input</code>&nbsp;string:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"input"</span><span class="p">:</span> <span class="s2">"Data to encode"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Use the&nbsp;<code>States.Base64Encode</code>&nbsp;function to encode
the&nbsp;<code>input</code>&nbsp;string as a MIME Base64 string:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"base64.$"</span><span class="p">:</span> <span class="s2">"States.Base64Encode($.input)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>The&nbsp;<code>States.Base64Encode</code>&nbsp;function returns the following encoded data in
response:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"base64"</span><span class="p">:</span> <span class="s2">"RGF0YSB0byBlbmNvZGU="</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesbase64decode">States.Base64Decode</h4>
<p>Use the&nbsp;<code>States.Base64Decode</code>&nbsp;intrinsic function to decode data based on
MIME Base64 decoding scheme. For example, use this function to pass data to
other AWS services without using a Lambda function.</p>
<p>This function takes a Base64 encoded data string of up to 10,000
characters to decode as its only argument.</p>
<p>For example, given the following input:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"base64"</span><span class="p">:</span> <span class="s2">"RGF0YSB0byBlbmNvZGU="</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Use the&nbsp;<code>States.Base64Decode</code>&nbsp;function to decode the base64
string to a human-readable string:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"data.$"</span><span class="p">:</span> <span class="s2">"States.Base64Decode($.base64)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>The&nbsp;<code>States.Base64Decode function</code>&nbsp;would return the following decoded
data in response:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"data"</span><span class="p">:</span> <span class="s2">"Decoded data"</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="stateshash">States.Hash</h4>
<p>Use the&nbsp;<code>States.Hash</code>&nbsp;intrinsic function to calculate the hash value of
a given input. For example, use this function to pass data to other AWS
services without using a Lambda function.</p>
<p>This function takes two arguments. The first argument is the data to
hash. The length of the stringified version of the first argument must
be 10,000 characters or less.  The second argument is the hashing
algorithm to use to perform the hash calculation.</p>
<p>The hashing algorithm can be any of the following
algorithms:</p>
<ul>
<li>MD5</li>
<li>SHA-1</li>
<li>SHA-256</li>
<li>SHA-384</li>
<li>SHA-512</li>
</ul>
<p>For example, given the&nbsp;<code>Data</code>&nbsp;string and a specific&nbsp;<code>Algorithm</code>:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"Data"</span><span class="p">:</span> <span class="s2">"input data"</span><span class="p">,</span> <span class="nt">"Algorithm"</span><span class="p">:</span> <span class="s2">"SHA-1"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Use the&nbsp;<code>States.Hash</code>&nbsp;function to calculate the hash value:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"output.$"</span><span class="p">:</span> <span class="s2">"States.Hash($.Data, $.Algorithm)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>The&nbsp;<code>States.Hash</code>&nbsp;function returns the following hash value in response:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"output"</span><span class="p">:</span> <span class="s2">"aaff4a450a104cd177d28d18d7485e8cae074b7"</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesjsonmerge">States.JsonMerge</h4>
<p>Use the&nbsp;<code>States.JsonMerge</code>&nbsp;intrinsic function to merge two JSON objects
into a single object. This function takes three arguments. The first two
arguments are the JSON objects to merge.  The third argument
is a boolean value that is <code>false</code> to do a shallow merge, and <code>true</code> to do
a deep merge.</p>
<p>In shallow
mode, if the same field name exists in both JSON objects, the latter object's
field overrides the field with the same name in the first object.</p>
<p>In deep mode, if the same field name exists in both JSON objects, and
both fields are themselves JSON objects, the function merges them.
Deep mode repeats this process recursively.</p>
<p>For example, use the&nbsp;<code>States.JsonMerge</code>&nbsp;function to merge the
following JSON arrays that both have a field named&nbsp;<code>a</code>.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> 
</span></span><span class="line"><span class="cl">  <span class="nt">"json1"</span><span class="p">:</span> <span class="p">{</span> <span class="nt">"a"</span><span class="p">:</span> <span class="p">{</span><span class="nt">"a1"</span><span class="p">:</span> <span class="mi">1</span><span class="p">,</span> <span class="nt">"a2"</span><span class="p">:</span> <span class="mi">2</span><span class="p">},</span> <span class="nt">"b"</span><span class="p">:</span> <span class="mi">2</span><span class="p">,</span> <span class="p">},</span>
</span></span><span class="line"><span class="cl">  <span class="nt">"json2"</span><span class="p">:</span> <span class="p">{</span> <span class="nt">"a"</span><span class="p">:</span> <span class="p">{</span><span class="nt">"a3"</span><span class="p">:</span> <span class="mi">1</span><span class="p">,</span> <span class="nt">"a4"</span><span class="p">:</span> <span class="mi">2</span><span class="p">},</span> <span class="nt">"c"</span><span class="p">:</span> <span class="mi">3</span> <span class="p">}</span>
</span></span><span class="line"><span class="cl"><span class="p">}</span>
</span></span></code></pre></div>
<p>Specify the <code>json1</code> and <code>json2</code> arrays as inputs in
the&nbsp;<code>States.JasonMerge</code>&nbsp;function to merge them together:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"output.$"</span><span class="p">:</span> <span class="s2">"States.JsonMerge($.json1, $.json2, false)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>The&nbsp;<code>States.JsonMerge</code>&nbsp;returns the following merged JSON object as
result. In the merged JSON object&nbsp;<code>output</code>, the&nbsp;<code>json2</code>&nbsp;object's
field&nbsp;<code>a</code>&nbsp;replaces the&nbsp;<code>json1</code>&nbsp;object's field&nbsp;<code>a</code>. Also, the nested object
in&nbsp;<code>json1</code>&nbsp;object's field&nbsp;<code>a</code>&nbsp;is discarded because shallow mode does not
support merging nested objects.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"output"</span><span class="p">:</span> <span class="p">{</span> <span class="nt">"a"</span><span class="p">:</span> <span class="p">{</span><span class="nt">"a3"</span><span class="p">:</span> <span class="mi">1</span><span class="p">,</span> <span class="nt">"a4"</span><span class="p">:</span> <span class="mi">2</span><span class="p">},</span> <span class="nt">"b"</span><span class="p">:</span> <span class="mi">2</span><span class="p">,</span> <span class="nt">"c"</span><span class="p">:</span> <span class="mi">3</span> <span class="p">}</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesmathrandom">States.MathRandom</h4>
<p>Use the&nbsp;<code>States.MathRandom</code>&nbsp;intrinsic function to return a random number
between the specified start and end number. For example, use
this function to distribute a specific task between two or more
resources.</p>
<p>This function takes three arguments. The first argument is the start
number, the second argument is the end number, and the last argument
controls the seed value. The seed value argument is optional.</p>
<p>Each use of this function with the same seed value will produce the
same result.</p>
<p>Important</p>
<p>Because the&nbsp;<code>States.MathRandom</code>&nbsp;function does not return
cryptographically secure random numbers, we recommend not to use
it for security sensitive applications.</p>
<p><strong>Input validation</strong></p>
<ul>
<li>The start number and end number MUST be integers.</li>
</ul>
<p>For example, to generate a random number from between one and 999,
use the following input values:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"start"</span><span class="p">:</span> <span class="mi">1</span><span class="p">,</span> <span class="nt">"end"</span><span class="p">:</span> <span class="mi">999</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>To generate the random number, provide the&nbsp;<code>start</code>&nbsp;and&nbsp;<code>end</code>&nbsp;values to
the&nbsp;<code>States.MathRandom</code>&nbsp;function:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"random.$"</span><span class="p">:</span> <span class="s2">"States.MathRandom($.start, $.end)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>The&nbsp;<code>States.MathRandom</code>&nbsp;function returns the following random number as
a response:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"random"</span><span class="p">:</span> <span class="mi">456</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesmathadd">States.MathAdd</h4>
<p>Use the&nbsp;<code>States.MathAdd</code>&nbsp;intrinsic function to return the sum of two
numbers. For example, use this function to increment values
inside a loop without invoking a Lambda function.</p>
<p><strong>Input validation</strong></p>
<ul>
<li>All arguments MUST be integers.</li>
</ul>
<p>For example, use the following values to subtract one from 111:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"value1"</span><span class="p">:</span> <span class="mi">111</span><span class="p">,</span> <span class="nt">"step"</span><span class="p">:</span> <span class="mi">-1</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Then, use the&nbsp;<code>States.MathAdd</code>&nbsp;function defining&nbsp;<code>value1</code>&nbsp;as the
starting value, and&nbsp;<code>step</code>&nbsp;as the value to increment&nbsp;<code>value1</code>&nbsp;by:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"value1.$"</span><span class="p">:</span> <span class="s2">"States.MathAdd($.value1, $.step)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>The&nbsp;<code>States.MathAdd</code>&nbsp;function would return the following number in
response:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"value1"</span><span class="p">:</span> <span class="mi">110</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesstringsplit">States.StringSplit</h4>
<p>Use the&nbsp;<code>States.StringSplit</code>&nbsp;intrinsic function to split a string into
an array of values. This function takes two arguments. The first argument
is a string and the second argument is the delimiting character that the
function will use to divide the string.</p>
<p>For example, use&nbsp;<code>States.StringSplit</code>&nbsp;to divide the
following&nbsp;<code>inputString</code>, which contains a series of comma separated
values:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"inputString"</span><span class="p">:</span> <span class="s2">"1,2,3,4,5"</span><span class="p">,</span> <span class="nt">"splitter"</span><span class="p">:</span> <span class="s2">","</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>Use the&nbsp;<code>States.StringSplit</code>&nbsp;function and define&nbsp;<code>inputString</code>&nbsp;as the
first argument, and the delimiting character&nbsp;<code>splitter</code>&nbsp;as the second
argument:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"array.$"</span><span class="p">:</span> <span class="s2">"States.StringSplit($.inputString, $.splitter)"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>The&nbsp;<code>States.StringSplit</code>&nbsp;function returns the following string array as
result:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"array"</span><span class="p">:</span> <span class="p">[</span><span class="s2">"1"</span><span class="p">,</span><span class="s2">"2"</span><span class="p">,</span><span class="s2">"3"</span><span class="p">,</span><span class="s2">"4"</span><span class="p">,</span><span class="s2">"5"</span><span class="p">]</span> <span class="p">}</span>
</span></span></code></pre></div>
<h4 id="statesuuid">States.UUID</h4>
<p>Use the&nbsp;<code>States.UUID</code>&nbsp;intrinsic function to return a version 4
universally unique identifier (v4 UUID) generated using random numbers.
For example, use this function to call other AWS services or
resources that need a UUID parameter or insert items in a DynamoDB
table.</p>
<p>The&nbsp;<code>States.UUID</code>&nbsp;function is called with no arguments specified:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"uuid.$"</span><span class="p">:</span> <span class="s2">"States.UUID()"</span> <span class="p">}</span>
</span></span></code></pre></div>
<p>The function returns a randomly generated UUID, for
example:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-json" data-lang="json"><span class="line"><span class="cl"><span class="p">{</span> <span class="nt">"uuid"</span><span class="p">:</span> <span class="s2">"ca4c1140-dcc1-40cd-ad05-7b4aa23df4a8"</span> <span class="p">}</span>
</span></span></code></pre></div>