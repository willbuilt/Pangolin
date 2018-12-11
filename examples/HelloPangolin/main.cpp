#include <pangolin/display/display.h>
#include <pangolin/display/view.h>
#include <pangolin/handler/handler.h>
#include <pangolin/gl/gldraw.h>
#include <pangolin/packet2/PacketLog.h>
#include <pangolin/packet2/RandomFile.h>
#include <pangolin/image/managed_image.h>
#include <pangolin/utils/timer.h>
#include <pangolin/packet2/RandomFile.h>

int main( int argc, char** argv )
{
    using namespace pangolin;

    //    pangolin::TestNew();

    ManagedImage<uint8_t> src(1024,1024);

    RandomFile f("test.bin");
    for(size_t i=0; i < 1000; ++i) {
        f.Append(std::shared_ptr<uint8_t>(src.ptr, [](uint8_t*){}), src.SizeBytes());
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto data = f.Get(0,10);
    data.get()[4] = 4;
    
    return 0;
}
