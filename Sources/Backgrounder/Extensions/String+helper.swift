import Foundation

extension String {
    /// Instantiates a class from a string.  Note that the namespace
    /// must also be included.  For example, a class in your app called
    /// "TestClass" would be
    ///
    ///     let klass = "App.TestClass".toClass
    ///
    var toClass: AnyClass? {
        return NSClassFromString(self)
    }

    /// Converts a Ruby class name to Swift
    ///
    /// - note: Automatically add the prefix "App" if no prefix was included.
    ///   This is an attempt at providing default compatibility with Sidekiq
    ///   by not requiring the user to add "module App" in their Ruby code. If
    ///   the user changes the default name of their Vapor project to something
    ///   other than "App" then they will be required to wrap their Ruby code
    ///   accordingly.
    ///
    var fromRuby: String {
        let string = self.replacingOccurrences(of: "::", with: ".")
        return !string.contains(".") ? "App.\(string)" : string
    }
    
    /// Converts a Swift class name to Ruby
    ///
    /// - note: Automatically remove the prefix "App" if it is present.
    ///   This is an attempt at providing default compatibility with Sidekiq
    ///   by not requiring the user to add "module App" in their Ruby code. If
    ///   the user changes the default name of their Vapor project to something
    ///   other than "App" then they will be required to wrap their Ruby code
    ///   accordingly.
    ///
    var toRuby: String {
        let string = self.replacingOccurrences(of: ".", with: "::")
        return string.starts(with: "App::") ? string.replacingOccurrences(of: "App::", with: "") : string
    }
}
