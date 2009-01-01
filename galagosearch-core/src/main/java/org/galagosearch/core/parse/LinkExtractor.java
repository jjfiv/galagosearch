// BSD License (http://www.galagosearch.org/license)
package org.galagosearch.core.parse;

import java.io.IOException;
import java.net.URL;
import org.galagosearch.core.types.ExtractedLink;
import org.galagosearch.tupleflow.InputClass;
import org.galagosearch.tupleflow.OutputClass;
import org.galagosearch.tupleflow.StandardStep;
import org.galagosearch.tupleflow.TupleFlowParameters;
import org.galagosearch.tupleflow.execution.Verified;

/**
 * Extracts links from documents (anchor text, URLs).
 * 
 * @author trevor
 */
@Verified
@InputClass(className = "org.galagosearch.core.parse.Document")
@OutputClass(className = "org.galagosearch.core.types.ExtractedLink")
public class LinkExtractor extends StandardStep<Document, ExtractedLink> {
    private boolean acceptLocalLinks;
    private boolean acceptNoFollowLinks;

    public LinkExtractor(TupleFlowParameters parameters) {
        acceptLocalLinks = parameters.getXML().get("acceptLocalLinks", false);
        acceptNoFollowLinks = parameters.getXML().get("acceptNoFollowLinks", false);
    }

    public String scrubUrl(String url) {
        // remove a leading pound sign
        if (url.charAt(url.length() - 1) == '#') {
            url = url.substring(0, url.length() - 1);        // make it lowercase
        }
        url = url.toLowerCase();

        // remove a port number, if it's the default number
        url = url.replace(":80/", "/");
        if (url.endsWith(":80")) {
            url = url.replace(":80", "");
        }
        // remove trailing slashes
        while (url.charAt(url.length() - 1) == '/') {
            url = url.substring(0, url.length() - 1);
        }
        return url;
    }

    public void process(Document document) throws IOException {
        String sourceUrl = document.metadata.get("url");

        if (sourceUrl == null) {
            return;
        }
        URL base = new URL(sourceUrl);

        for (Tag t : document.tags) {
            if (t.name.equals("base")) {
                try {
                    base = new URL(base, t.attributes.get("href"));
                } catch (Exception e) {
                    // this can happen when the link protocol is unknown
                    base = new URL(sourceUrl);
                    continue;
                }
            } else if (t.name.equals("a")) {
                String destSpec = t.attributes.get("href");
                URL destUrlObject = null;
                String destUrl = null;

                try {
                    destUrlObject = new URL(base, destSpec);
                    destUrl = destUrlObject.toString();
                } catch (Exception e) {
                    // this can happen when the link protocol is unknown
                    continue;
                }

                boolean linkIsLocal = destUrlObject.getHost().equals(base.getHost());

                // if we're filtering out local links, there's no need to continue
                if (linkIsLocal && acceptLocalLinks == false) {
                    continue;
                }
                ExtractedLink link = new ExtractedLink();

                link.srcUrl = sourceUrl;
                link.destUrl = scrubUrl(destUrl);

                StringBuilder builder = new StringBuilder();

                for (int i = t.begin; i < t.end && i < document.terms.size(); i++) {
                    String term = document.terms.get(i);

                    if (term != null) {
                        builder.append(term);
                        builder.append(' ');
                    }
                }

                link.anchorText = builder.toString().trim();

                if (t.attributes.containsKey("rel") && t.attributes.get("rel").equals("nofollow")) {
                    link.noFollow = true;
                } else {
                    link.noFollow = false;
                }

                boolean acceptable = (acceptNoFollowLinks || link.noFollow == false) &&
                        (acceptLocalLinks || linkIsLocal == false);

                if (acceptable) {
                    processor.process(link);
                }
            }
        }
    }

    public Class<Document> getInputClass() {
        return Document.class;
    }

    public Class<ExtractedLink> getOutputClass() {
        return ExtractedLink.class;
    }
}
