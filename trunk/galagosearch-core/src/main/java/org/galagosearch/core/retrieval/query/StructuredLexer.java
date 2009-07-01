// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.retrieval.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 *
 * @author trevor
 */
public class StructuredLexer {
    public static class Token {
        public Token(String text, int position) {
            this.text = text;
            this.position = position;
        }

        @Override
        public String toString() {
            return text + ":" + position;
        }

        public String text;
        public int position;
    }

    private static void addQuotedTokens(String quotedString, ArrayList<Token> tokens, int offset) {
        tokens.add(new Token("\"", offset));
        offset++;
        int start = -1;
        boolean wasSpace = true;
        int j;
        for (j = 0; j < quotedString.length(); j++) {
            char c = quotedString.charAt(j);
            boolean isSpace = Character.isSpaceChar(c);
  
            if (isSpace) {
                if (!wasSpace && start >= 0) {
                    tokens.add(new Token(quotedString.substring(start, j), start+offset));
                }
                start = -1;
            } else if (wasSpace) {
                start = j;
            }

            wasSpace = isSpace;
        }

        // emit final token
        if (start > 0 && start != j) {
          tokens.add(new Token(quotedString.substring(start), start+offset));
        }

        tokens.add(new Token("\"", offset+j));
    }

    public static List<Token> tokens(String query) throws IOException {
        ArrayList<Token> tokens = new ArrayList<Token>();
        HashSet<Character> tokenCharacters = new HashSet<Character>();
        tokenCharacters.add('#');
        tokenCharacters.add(':');
        tokenCharacters.add('.');
        tokenCharacters.add('=');
        tokenCharacters.add(')');
        tokenCharacters.add('(');
        int start = 0;

        for (int i = 0; i < query.length(); ++i) {
            char c = query.charAt(i);
            boolean special = tokenCharacters.contains(c) || c == '@' || c == '"';
            boolean isSpace = Character.isSpaceChar(c);
            
            if (special || isSpace) {
                if (start != i) {
                    tokens.add(new Token(query.substring(start, i), start));
                }

                if (c == '@') {
                    if (i+1 < query.length()) {
                        char escapeChar = query.charAt(i+1);
                        int endChar = query.indexOf(escapeChar, i+2);

                        if (endChar < 0) {
                            throw new IOException("Lex failure: No end found to '@' escape sequence.");
                        }

                        tokens.add(new Token("@", i));
                        tokens.add(new Token(query.substring(i+2, endChar), i));
                        i = endChar;
                    } else {
                        throw new IOException("Lex failure: '@' at end of input sequence.");
                    }
                } else if (c == '"') {
                    // find the end of this escape sequence and break on spaces
                    int endChar = query.indexOf('"', i+1);
                    if (endChar < 0) {
                        throw new IOException("Lex failure: No ending quote found.");
                    }

                    String quotedString = query.substring(i+1, endChar);
                    addQuotedTokens(quotedString, tokens, i);
                    i = endChar;
                } else if (!isSpace) {
                    tokens.add(new Token(Character.toString(c), i));
                }

                start = i+1;
            }
        }

        if (start != query.length()) {
            tokens.add(new Token(query.substring(start), start));
        }
        return tokens;
    }
}
