#include <iostream>
#include <cstring>
#include <iomanip>
#include <vector>
#include <algorithm>
#include "../src/float_utils.h"

using namespace std;
using namespace xtree;

void printBits(int32_t n) {
    for (int i = 31; i >= 0; i--) {
        cout << ((n >> i) & 1);
        if (i == 31 || i == 23) cout << " ";  // Separate sign and exponent
    }
}

void testFloat(float f) {
    int32_t raw;
    memcpy(&raw, &f, sizeof(float));
    int32_t sortable = floatToSortableInt(f);
    float back = sortableIntToFloat(sortable);
    
    cout << fixed << setprecision(6);
    cout << "Float: " << f << endl;
    cout << "Raw bits:      ";
    printBits(raw);
    cout << " (" << raw << ")" << endl;
    cout << "Sortable bits: ";
    printBits(sortable);
    cout << " (" << sortable << ")" << endl;
    cout << "Back to float: " << back << endl;
    cout << "---" << endl;
}

int main() {
    cout << "Testing float to sortable int conversion:" << endl << endl;
    
    testFloat(0.0f);
    testFloat(-0.0f);
    testFloat(1.0f);
    testFloat(-1.0f);
    testFloat(100.0f);
    testFloat(-100.0f);
    
    // Test sorting order
    cout << endl << "Testing sorting order:" << endl;
    float values[] = {-100.0f, -1.0f, -0.0f, 0.0f, 1.0f, 100.0f};
    vector<pair<int32_t, float>> sortables;
    for (int i = 0; i < 6; i++) {
        int32_t sortable = floatToSortableInt(values[i]);
        sortables.push_back({sortable, values[i]});
        cout << values[i] << " -> " << sortable << endl;
    }
    
    // Sort by sortable int
    sort(sortables.begin(), sortables.end());
    cout << endl << "After sorting by sortable int:" << endl;
    for (const auto& p : sortables) {
        cout << p.second << " (sortable: " << p.first << ")" << endl;
    }
    
    return 0;
}