/*
 * BSD License (http://www.galagosearch.org/license)

 */

package org.galagosearch.core.retrieval.structured;

import java.util.Set;

/**
 * Indicates that an iterator can have modifiers attached to it.
 * This would work really well as a Scala trait or a Ruby module,
 * but Java doesn't play that way.
 *
 * @author irmarc
 */
public interface ModifiableIterator {
    public void addModifier(String k, Object m);

    public Set<String> getAvailableModifiers();

    public boolean hasModifier(String key) ;

    public Object getModifier(String modKey);
}
