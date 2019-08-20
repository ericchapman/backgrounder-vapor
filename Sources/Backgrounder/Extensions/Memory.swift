import Foundation

public struct Memory {
    
    public enum MemoryError: Error {
        case error(string: String)
    }
    
    /// Returns the memory usage in MB
    ///
    public static func usage() throws -> Int {
        var taskInfo = mach_task_basic_info()
        var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size)/4
        let kerr: kern_return_t = withUnsafeMutablePointer(to: &taskInfo) {
            $0.withMemoryRebound(to: integer_t.self, capacity: 1) {
                task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), $0, &count)
            }
        }
        
        if kerr == KERN_SUCCESS {
            return Int(taskInfo.resident_size >> 20)
        }
        else {
            throw MemoryError.error(string: "Error with task_info(): " +
                (String(cString: mach_error_string(kerr), encoding: String.Encoding.ascii) ?? "unknown error"))
        }
    }
}
