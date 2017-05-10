package bayou

import (
    "strconv"
    "testing"
    "time"
)

/*************************
 *    HELPER METHODS     *
 *************************/

/****************************
 *    PERFORMANCE TESTS     *
 ****************************/

/* Tests: IDK                          *
 * Result: something something linear? */
func TestWritePerformance(t *testing.T) {
    servers, clients := createBayouNetwork("test_speed", 10)
    _ = servers // uh, okay? (Lance)
    defer removeBayouNetwork(servers, clients)

    var i int
    for i = 0; i < 5; i++ {
        servers[0].IsPrimary = true
        // Test Writes
        n := 50 * (2 << uint(i))
        start := time.Now()
        for j := 0; j < n; j++ {
            clients[i].ClaimRoom("R" + strconv.Itoa(j), 1, 1)
        }
        elapsed := time.Since(start)
        Log.Printf("Primary, %d took %s\n", n, elapsed)
    }

    for i = 0; i < 5; i++ {
        // Test Writes
        n := 50 * (2 << uint(i))
        start := time.Now()
        for j := 0; j < n; j++ {
            clients[5 + i].ClaimRoom("R" + strconv.Itoa(j), 1, 1)
        }
        elapsed := time.Since(start)
        Log.Printf("Not Primary, %d took %s\n", n, elapsed)
    }
}
