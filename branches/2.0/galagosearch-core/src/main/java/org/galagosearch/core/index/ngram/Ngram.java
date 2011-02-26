// BSD License (http://www.galagosearch.org/license)

package org.galagosearch.core.index.ngram;

/*
 * n-gram datatype
 *
 * @author sjh
 */
public class Ngram {
  
  public Ngram(){
    
  }
  
  public Ngram(int file, long filePosition, int document, int position, byte[] ngram){
    this.file = file;
    this.filePosition = filePosition;
    this.document = document;
    this.position = position;
    this.ngram = ngram;
  }
  
  // location in file for filtering (SpaceEfficient) //
  public int file;
  public long filePosition;
  
  // indexing data //
  public int document;
  public byte[] ngram;
  public int position;

}
