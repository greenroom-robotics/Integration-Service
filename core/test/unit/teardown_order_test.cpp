// Regression test for instance teardown order (sc-27765: is-can segfaulted on
// shutdown). SystemHandles get a raw callback pointer and may call it from their
// own threads until they are destroyed, so:
//  - the callback must still be callable while SystemHandles are destroyed
//    (built with ASan: calling a freed callback aborts the test), and
//  - the publishers it captured must already be released by then, because a
//    publisher may depend on its own SystemHandle (e.g. plugin-built types).

#include <is/core/Instance.hpp>
#include <is/systemhandle/SystemHandle.hpp>

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

namespace {

// Only the route's subscription callback owns the publisher, so this expires
// exactly when the callback's captured state is released.
std::weak_ptr<eprosima::is::TopicPublisher> g_publisher;
bool g_subscriber_destroyed = false;
bool g_publisher_released_before_teardown = false;

class NullPublisher : public eprosima::is::TopicPublisher
{
public:

    bool publish(
            const eprosima::xtypes::DynamicData&) override
    {
        return true;
    }

};

eprosima::xtypes::StructType probe_type()
{
    eprosima::xtypes::StructType type("probe_msg");
    type.add_member("data", eprosima::xtypes::primitive_type<uint32_t>());
    return type;
}

class TeardownProbe : public eprosima::is::TopicSystem
{
public:

    ~TeardownProbe() override
    {
        if (callback_ != nullptr)
        {
            g_subscriber_destroyed = true;
            g_publisher_released_before_teardown = g_publisher.expired();

            // What a still-running RX thread does during teardown.
            const auto type = probe_type();
            (*callback_)(eprosima::xtypes::DynamicData(type), nullptr);
        }
    }

    bool configure(
            const eprosima::is::core::RequiredTypes&,
            const YAML::Node&,
            eprosima::is::TypeRegistry& type_registry) override
    {
        type_registry.emplace("probe_msg", probe_type());
        return true;
    }

    bool okay() const override
    {
        return true;
    }

    bool spin_once() override
    {
        return true;
    }

    bool subscribe(
            const std::string&,
            const eprosima::xtypes::DynamicType&,
            SubscriptionCallback* callback,
            const YAML::Node&) override
    {
        callback_ = callback;
        return true;
    }

    bool is_internal_message(
            void*) override
    {
        return false;
    }

    std::shared_ptr<eprosima::is::TopicPublisher> advertise(
            const std::string&,
            const eprosima::xtypes::DynamicType&,
            const YAML::Node&) override
    {
        auto publisher = std::make_shared<NullPublisher>();
        g_publisher = publisher;
        return publisher;
    }

private:

    SubscriptionCallback* callback_ = nullptr;
};

} // namespace

IS_REGISTER_SYSTEM("teardown_probe", TeardownProbe)

TEST(Instance, Callbacks_outlive_system_handles_but_not_their_publishers)
{
    // The probe is linked into this binary, so its .mix only needs to exist.
    const auto mix_dir = std::filesystem::temp_directory_path() / "is_core_teardown_probe";
    std::filesystem::create_directories(mix_dir);
    std::ofstream(mix_dir / "teardown_probe.mix") << "{}\n";

    const YAML::Node config = YAML::Load(R"(
systems:
  from_probe: { type: teardown_probe }
  to_probe: { type: teardown_probe }
routes:
  probe_route: { from: from_probe, to: to_probe }
topics:
  probe_topic: { type: "probe_msg", route: probe_route }
)");

    {
        auto handle = eprosima::is::run_instance(
            config, {}, {{"teardown_probe", {mix_dir.string()}}});
        ASSERT_TRUE(handle.running());
        ASSERT_FALSE(g_publisher.expired());
        handle.quit().wait();
    }

    ASSERT_TRUE(g_subscriber_destroyed);
    EXPECT_TRUE(g_publisher_released_before_teardown);
}

int main(
        int argc,
        char** argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
