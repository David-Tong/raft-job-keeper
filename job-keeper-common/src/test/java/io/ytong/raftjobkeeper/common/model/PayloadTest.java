package io.ytong.raftjobkeeper.common.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PayloadTest {
    @Test
    void nullCollectionsBecomeEmpty() {
        Payload payload = new Payload("shell", "echo", null, null);
        assertEquals(List.of(), payload.args());
        assertEquals(Map.of(), payload.env());
    }

    @Test
    void collectionsAreDefensivelyCopiedAndImmutable() {
        var args = new ArrayList<>(List.of("hello"));
        var env = new HashMap<>(Map.of("MODE", "test"));
        Payload payload = new Payload("shell", "echo", args, env);
        args.add("changed");
        env.put("MODE", "changed");
        assertEquals(List.of("hello"), payload.args());
        assertEquals(Map.of("MODE", "test"), payload.env());
        assertThrows(UnsupportedOperationException.class, () -> payload.args().add("extra"));
        assertThrows(UnsupportedOperationException.class, () -> payload.env().put("X", "Y"));
    }
}
