/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.galagosearch.core.index.geometric;

import java.io.File;
import java.io.IOException;
import org.galagosearch.tupleflow.Parameters;
import org.galagosearch.tupleflow.Utility;

/**
 * 
 * @author sjh
 */
public class CheckPointHandler {
  private String path;
  
  public void setDirectory(String dir){
    this.path = dir + File.separator + "checkpoint" ;
    Utility.makeParentDirectories( path );
  }
  
  public void saveCheckpoint(Parameters checkpoint) throws IOException {
    Utility.copyStringToFile( checkpoint.toString() , new File(path) );
  }
  
  public Parameters getRestore() throws IOException {
    return new Parameters( new File(path) );
  }
}
