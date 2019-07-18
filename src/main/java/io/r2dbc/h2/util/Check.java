package io.r2dbc.h2.util;

/**
 * Utilities for inspecting some conditions.
 */
public final class Check {

    private Check() {
    }

    /**
     * Checks if the class is found in the current class loader.
     *
     * @param fullyQualifiedClassName the fully qualified name of the desired class
     * @return true, if the class is found
     */
    public static boolean findClass(final String fullyQualifiedClassName) {
      try {
          Class.forName(fullyQualifiedClassName);
          return true;
      } catch (ClassNotFoundException e) {
          return false;
      }
    }
}
