@testable import Backgrounder
import Vapor
import XCTest

final class RedisTests: RedisTestCase {
    
    enum TempError: Error {
        case error
    }
    
    func testConnectionAutoRelease() throws {
        var exceptionTriggered = false
        var tempConnection: RedisConnection? = nil
        var value: Int? = nil
        
        // Method to run the test
        func runTest(klass: RedisConnection.Type, exception: Bool) {
            exceptionTriggered = false
            tempConnection = nil
            value = nil
            
            do {
                value = try klass.openWithAutoClose(on: self.app, as: .redis) { (conn: RedisConnection) -> Future<Int> in
                    tempConnection = conn
                    if exception {
                        throw TempError.error
                    }
                    return self.app.eventLoop.newSucceededFuture(result: 1)
                    }.catch { error in
                        exceptionTriggered = true
                    }.wait()
            } catch {}
        }
        
        // Try Normal Value
        runTest(klass: RedisConnection.self, exception: false)
        XCTAssertEqual(exceptionTriggered, false)
        XCTAssertEqual(tempConnection?.isClosed, true)
        XCTAssertEqual(value, 1)
       
        // Try Normal Exeption
        runTest(klass: RedisConnection.self, exception: true)
        XCTAssertEqual(exceptionTriggered, true)
        XCTAssertEqual(tempConnection?.isClosed, true)
        XCTAssertNil(value)
        
        // Try Pooled Value
        runTest(klass: RedisPooledConnection.self, exception: false)
        XCTAssertEqual(exceptionTriggered, false)
        XCTAssertEqual(tempConnection?.isClosed, true)
        XCTAssertEqual(value, 1)
        
        // Try Pooled Exeption
        runTest(klass: RedisPooledConnection.self, exception: true)
        XCTAssertEqual(exceptionTriggered, true)
        XCTAssertEqual(tempConnection?.isClosed, true)
        XCTAssertNil(value)
    }
    
    static var allTests = [
        ("testConnectionAutoRelease", testConnectionAutoRelease),
    ]
}
