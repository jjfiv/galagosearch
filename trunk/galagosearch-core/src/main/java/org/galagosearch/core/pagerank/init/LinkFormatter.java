// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.pagerank.init;

import org.galagosearch.core.types.ExtractedLink;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.execution.Verified;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;

/**
 * re-formats link urls
 * 
 * @author schiu, sjh
 */
@Verified
@InputClass(className = "org.galagosearch.core.types.ExtractedLink")
@OutputClass(className = "org.galagosearch.core.types.ExtractedLink", order = {"+destUrl"})
public class LinkFormatter extends StandardStep<ExtractedLink, ExtractedLink> {

  public void process(ExtractedLink el) throws IOException {
    el.destUrl = el.destUrl.replaceAll("/../articles", "");
    el.destUrl = el.destUrl.replaceAll("#.*", "");
    try {
      el.destUrl = URLDecoder.decode(el.destUrl);
    } catch (IllegalArgumentException i) {
      //for testing/debugging purposes, safe to ignore in normal use
      //		System.err.println("URL: " + el.destUrl + "\ncaused error: " + i.getMessage());
    }
    el.destUrl = URLEncoder.encode(el.destUrl);
    el.srcUrl = URLEncoder.encode(el.srcUrl);
    processor.process(el);

  }
}
