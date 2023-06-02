package com.github.ambry.cloud;

import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenFactory;
import java.io.DataInputStream;
import java.io.IOException;


public class RecoveryTokenFactory implements FindTokenFactory {
  @Override
  public FindToken getFindToken(DataInputStream stream) throws IOException {
    return RecoveryToken.fromBytes(stream);
  }

  @Override
  public FindToken getNewFindToken() {
    return new RecoveryToken();
  }
}
