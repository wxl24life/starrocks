// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exprs/ai/ai_provider_registry.h"

#include "exprs/ai/openai_compatible_provider.h"

namespace starrocks {

AIProviderRegistry::AIProviderRegistry() {
    auto openai = std::make_unique<OpenAICompatibleProvider>();
    _default = openai.get();
    _providers.emplace("openai_compatible", std::move(openai));
}

AIProviderRegistry* AIProviderRegistry::instance() {
    static AIProviderRegistry registry;
    return &registry;
}

void AIProviderRegistry::register_provider(const std::string& name, std::unique_ptr<AIProvider> provider) {
    auto it = _providers.find(name);
    bool replacing_default = (it != _providers.end() && it->second.get() == _default);
    _providers[name] = std::move(provider);
    if (replacing_default) {
        _default = _providers[name].get();
    }
}

AIProvider* AIProviderRegistry::get_provider(const std::string& name) const {
    if (name.empty()) return _default;
    auto it = _providers.find(name);
    return it != _providers.end() ? it->second.get() : _default;
}

} // namespace starrocks
