package com.real.cassandra.queue.pipes;

public enum PipeStatus {
    // the names are stored in the database, so don't change them. you can
    // changed the actually enum (ie MY_ACCT) all day long with no problems

    ACTIVE("A"),

    NOT_ACTIVE("N"),

    COMPLETED("C")

    ;

    private final String name;

    PipeStatus(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static PipeStatus getInstance(String name) {
        PipeStatus[] stArr = values();
        for (PipeStatus st : stArr) {
            if (st.getName().equals(name)) {
                return st;
            }
        }

        throw new IllegalArgumentException("No Transport ID with name, " + name);
    }

}
