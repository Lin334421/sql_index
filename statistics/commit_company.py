from statistics.ck_hook import CKServer
from statistics.config import clickhouse_server_info

"""
1、去掉机器人 数据量与原始数据相比可能会减少,而且机器人可能无法完全清除
2、

"""
repos = [
    {
        "owner": "Project-MONAI",
        "repo": "MONAI"
    },

    {
        "owner": "NVIDIA",
        "repo": "NeMo-Guardrails"
    },
    {
        "owner": "onnx",
        "repo": "onnx-tensorrt"
    },
    {
        "owner": "NVIDIA",
        "repo": "TransformerEngine"
    },
    {
        "owner": "pytorch",
        "repo": "TensorRT"
    },
    {
        "owner": "NVIDIA-Omniverse",
        "repo": "PhysX"
    },
    {
        "owner": "PixarAnimationStudios",
        "repo": "USD"
    },
    {
        "owner": "NVIDIA-Omniverse",
        "repo": "USD-proposals"
    },
    {
        "owner": "NVIDIA",
        "repo": "AMGX"
    },
    {
        "owner": "rapidsai",
        "repo": "rmm"
    },
    {
        "owner": "rapidsai",
        "repo": "raft"
    },
    {
        "owner": "rapidsai",
        "repo": "cuxfilter"
    },
    {
        "owner": "rapidsai",
        "repo": "cugraph"
    },
    {
        "owner": "rapidsai",
        "repo": "cudf"
    },
    {
        "owner": "CVCUDA",
        "repo": "CV-CUDA"
    },
    {
        "owner": "NVIDIA",
        "repo": "Megatron-LM"
    }
]
owner_repos = ['https://github.com/pytorch/ort', 'https://github.com/huggingface/EnergyStarAI', 'https://github.com/huggingface/knockknock', 'https://github.com/huggingface/RL-model-card-template', 'https://github.com/huggingface/temp-tailscale-action', 'https://github.com/huggingface/tokenizers', 'https://github.com/huggingface/hf-endpoints-emulator', 'https://github.com/pytorch/probot', 'https://github.com/apache/tvm', 'https://github.com/pytorch/captum', 'https://github.com/huggingface/candle-cublaslt', 'https://github.com/huggingface/collaborative-training-auth', 'https://github.com/postgres/postgres', 'https://github.com/denoland/deno', 'https://github.com/huggingface/neuralcoref', 'https://github.com/huggingface/helm-publish-action', 'https://github.com/huggingface/hugs-helm-chart', 'https://github.com/huggingface/ML-Agents-Training-Executables', 'https://github.com/pytorch/cppdocs', 'https://github.com/huggingface/torchMoji', 'https://github.com/huggingface/hf_benchmarks', 'https://github.com/denoland/std', 'https://github.com/huggingface/accelerate', 'https://github.com/huggingface/awesome-huggingface', 'https://github.com/pytorch/glow', 'https://github.com/pytorch/torchhub_testing', 'https://github.com/huggingface/lerobot', 'https://github.com/apache/arrow-rs', 'https://github.com/huggingface/Snowball-Target', 'https://github.com/pytorch/contrib', 'https://github.com/cilium/tetragon', 'https://github.com/huggingface/doc-build-dev', 'https://github.com/huggingface/model-evaluator', 'https://github.com/pandas-dev/pandas', 'https://github.com/llvm/circt', 'https://github.com/huggingface/large_language_model_training_playbook', 'https://github.com/huggingface/m4-logs', 'https://github.com/huggingface/huggingface_tianshou', 'https://github.com/huggingface/transformers.js', 'https://github.com/huggingface/huggingface_sb3', 'https://github.com/pytorch/pytorch.github.io', 'https://github.com/pytorch/hub', 'https://github.com/denoland/rusty_v8', 'https://github.com/pytorch/torchrec', 'https://github.com/pytorch/torchdistx', 'https://github.com/huggingface/peft', 'https://github.com/huggingface/diffusion-fast', 'https://github.com/apache/datafusion-ballista', 'https://github.com/huggingface/audio-transformers-course', 'https://github.com/huggingface/exporters', 'https://github.com/huggingface/efficient_scripts', 'https://github.com/pytorch/hydra-torch', 'https://github.com/llvm/llvm-test-suite', 'https://github.com/pytorch/data', 'https://github.com/huggingface/pytorch-image-models', 'https://gitlab.com/libeigen/eigen', 'https://github.com/huggingface/gsplat.js', 'https://github.com/pytorch/text', 'https://github.com/huggingface/parler-tts', 'https://github.com/huggingface/simulate', 'https://github.com/huggingface/evaluation-guidebook', 'https://github.com/rust-lang/rust', 'https://github.com/pytorch/torchchat', 'https://github.com/huggingface/chat-macOS', 'https://github.com/huggingface/gaia', 'https://github.com/huggingface/frp', 'https://github.com/huggingface/dataspeech', 'https://github.com/huggingface/nn_pruning', 'https://github.com/cilium/hubble', 'https://github.com/huggingface/personas', 'https://github.com/huggingface/hugs-docs', 'https://github.com/pytorch/android-demo-app', 'https://github.com/huggingface/swift-chat', 'https://github.com/pytorch/torchsnapshot', 'https://github.com/llvm/Polygeist', 'https://github.com/huggingface/huggingface-inference-toolkit', 'https://github.com/pytorch/tnt', 'https://github.com/pytorch/audio', 'https://github.com/pytorch/ossci-job-dsl', 'https://github.com/huggingface/hmtl', 'https://github.com/huggingface/optimum', 'https://github.com/huggingface/hf-workflows', 'https://github.com/huggingface/hfapi', 'https://github.com/huggingface/datablations', 'https://github.com/llvm/llvm-lnt', 'https://github.com/huggingface/transformers', 'https://github.com/pytorch/rfcs', 'https://github.com/OpenMathLib/OpenBLAS', 'https://github.com/huggingface/adversarialnlp', 'https://github.com/huggingface/olm-training', 'https://github.com/pytorch/executorch', 'https://github.com/pytorch/workshops', 'https://github.com/huggingface/ethics-scripts', 'https://github.com/huggingface/datasets-tagging', 'https://github.com/huggingface/spm_precompiled', 'https://github.com/rust-lang/rustc-perf', 'https://github.com/pytorch/pytorch', 'https://github.com/huggingface/naacl_transfer_learning_tutorial', 'https://github.com/huggingface/pytorch-openai-transformer-lm', 'https://github.com/huggingface/optimum-habana', 'https://github.com/pytorch/pytorch-ci-dockerfiles', 'https://github.com/huggingface/course', 'https://github.com/huggingface/model_card', 'https://github.com/etcd-io/auger', 'https://github.com/huggingface/setfit', 'https://github.com/huggingface/llm-intellij', 'https://github.com/pytorch/torchcodec', 'https://github.com/huggingface/docmatix', 'https://github.com/pytorch/torchtune', 'https://github.com/huggingface/swift-transformers', 'https://github.com/huggingface/bloom-jax-inference', 'https://github.com/torvalds/linux', 'https://github.com/huggingface/olm-datasets', 'https://github.com/pytorch/builder', 'https://github.com/huggingface/controlnet_aux', 'https://github.com/huggingface/data-is-better-together', 'https://github.com/huggingface/pytorch-pretrained-BigGAN', 'https://github.com/huggingface/chat-ui', 'https://github.com/denoland/fresh', 'https://github.com/pytorch/expecttest', 'https://github.com/pytorch/maskedtensor', 'https://github.com/pytorch/ios-demo-app', 'https://github.com/huggingface/paper-style-guide', 'https://github.com/llvm/llvm-project-staging', 'https://github.com/huggingface/llm-academy', 'https://github.com/huggingface/pixparse', 'https://github.com/pytorch/cpuinfo', 'https://github.com/huggingface/hub-docs', 'https://github.com/huggingface/api-inference-community', 'https://github.com/huggingface/optimum-furiosa', 'https://github.com/huggingface/alignment-handbook', 'https://github.com/huggingface/llm-ls', 'https://github.com/pytorch/translate', 'https://github.com/pytorch/test-infra', 'https://github.com/huggingface/tflite-android-transformers', 'https://github.com/huggingface/widgets-server', 'https://github.com/huggingface/bench_cluster', 'https://github.com/pytorch/ci-hud', 'https://github.com/huggingface/Unity-MLAgents-LoadFromHub-Assets', 'https://github.com/huggingface/huggingface-llama-recipes', 'https://github.com/huggingface/workshops', 'https://github.com/huggingface/awesome-papers', 'https://github.com/pytorch/ELF', 'https://github.com/numpy/numpy', 'https://github.com/pytorch/botorch', 'https://github.com/llvm/llvm-project', 'https://github.com/huggingface/nanotron', 'https://github.com/huggingface/100-times-faster-nlp', 'https://github.com/huggingface/neuralcoref-viz', 'https://github.com/pytorch/docs', 'https://github.com/huggingface/instruction-tuned-sd', 'https://github.com/pytorch/labeler-github-action', 'https://github.com/huggingface/text-generation-inference', 'https://github.com/huggingface/rasa_hmtl', 'https://github.com/pytorch/opacus', 'https://github.com/huggingface/coreml-examples', 'https://github.com/huggingface/huggingface.js', 'https://github.com/huggingface/peft-pytorch-conference', 'https://github.com/huggingface/evaluate', 'https://github.com/pytorch/torchdynamo', 'https://github.com/huggingface/rlhf-interface', 'https://github.com/huggingface/doc-build', 'https://github.com/huggingface/khipu_workshop', 'https://github.com/huggingface/chat-ui-android', 'https://github.com/huggingface/amused', 'https://github.com/pytorch/serve', 'https://github.com/huggingface/disaggregators', 'https://github.com/huggingface/swift-coreml-diffusers', 'https://github.com/pytorch/tensorpipe', 'https://github.com/huggingface/roots-search-tool', 'https://github.com/pytorch/pytorch-integration-testing', 'https://github.com/pytorch/nestedtensor', 'https://github.com/apache/datafusion', 'https://github.com/huggingface/speechbox', 'https://github.com/cilium/cilium', 'https://github.com/huggingface/hf-rocm-benchmark', 'https://github.com/microsoft/TypeScript', 'https://github.com/huggingface/diffusion-models-class', 'https://github.com/huggingface/semver-release-action', 'https://github.com/huggingface/Mongoku', 'https://github.com/pytorch/FBGEMM', 'https://github.com/huggingface/zapier', 'https://github.com/huggingface/hf-hub', 'https://github.com/huggingface/autotrain-example-datasets', 'https://github.com/huggingface/tune', 'https://github.com/pytorch/tensordict', 'https://github.com/huggingface/text-clustering', 'https://github.com/huggingface/community-events', 'https://github.com/huggingface/optimum-nvidia', 'https://github.com/huggingface/optimum-amd', 'https://github.com/huggingface/cookbook', 'https://github.com/huggingface/autogptq-index', 'https://github.com/pytorch/kineto', 'https://github.com/huggingface/sharp-transformers', 'https://github.com/huggingface/unity-api', 'https://github.com/huggingface/ms-build-mi300', 'https://github.com/huggingface/autotrain-advanced', 'https://github.com/huggingface/gguf-jinja-analysis', 'https://github.com/huggingface/dataset-viewer', 'https://github.com/huggingface/llm.nvim', 'https://github.com/huggingface/dedupe_estimator', 'https://github.com/huggingface/making-games-with-ai-course', 'https://github.com/huggingface/pyo3-special-method-derive', 'https://github.com/huggingface/autotrain-advanced-api', 'https://github.com/pytorch/rl', 'https://github.com/huggingface/candle', 'https://github.com/huggingface/optimum-benchmark', 'https://github.com/huggingface/gym-xarm', 'https://github.com/llvm/torch-mlir', 'https://github.com/pytorch/benchmark', 'https://github.com/huggingface/ethics-education', 'https://github.com/huggingface/node-question-answering', 'https://github.com/huggingface/transformers.js-examples', 'https://github.com/cilium/pwru', 'https://github.com/huggingface/sam2-studio', 'https://github.com/huggingface/pytorch_block_sparse', 'https://github.com/pytorch/TensorRT', 'https://github.com/huggingface/local-gemma', 'https://github.com/huggingface/tailscale-action', 'https://github.com/pytorch/extension-ffi', 'https://github.com/huggingface/block_movement_pruning', 'https://github.com/pytorch/torcharrow', 'https://github.com/pytorch/examples', 'https://github.com/pytorch/QNNPACK', 'https://github.com/pytorch/torcheval', 'https://github.com/huggingface/open-muse', 'https://github.com/huggingface/candle-layer-norm', 'https://github.com/huggingface/action-check-commits', 'https://github.com/huggingface/gym-pusht', 'https://github.com/huggingface/deep-rl-class', 'https://github.com/pytorch/csprng', 'https://github.com/nodejs/node', 'https://github.com/pytorch/accimage', 'https://github.com/huggingface/hf-endpoints-documentation', 'https://github.com/kubernetes/kubernetes', 'https://github.com/huggingface/ratchet', 'https://github.com/huggingface/huggingface_hub', 'https://github.com/huggingface/fineVideo', 'https://github.com/pytorch/elastic', 'https://github.com/huggingface/blog', 'https://github.com/huggingface/hub-js-utils', 'https://github.com/huggingface/lighteval', 'https://github.com/llvm/clangir', 'https://github.com/facebook/rocksdb', 'https://github.com/pytorch/vision', 'https://github.com/huggingface/candle-silu', 'https://github.com/huggingface/doc-builder', 'https://github.com/pytorch/dr-ci', 'https://github.com/huggingface/hffs', 'https://github.com/huggingface/distribution-v2', 'https://github.com/pytorch/torchtitan', 'https://github.com/pytorch/multipy', 'https://github.com/rust-lang/portable-simd', 'https://github.com/huggingface/Huggy', 'https://github.com/huggingface/speech-to-speech', 'https://github.com/huggingface/prettier-plugin-vertical-align', 'https://github.com/pytorch/functorch', 'https://github.com/huggingface/leaderboards', 'https://github.com/huggingface/neuralcoref-models', 'https://github.com/rust-lang/cargo', 'https://github.com/huggingface/Google-Cloud-Containers', 'https://github.com/huggingface/swift-coreml-transformers', 'https://github.com/huggingface/open_asr_leaderboard', 'https://github.com/huggingface/diarizers', 'https://github.com/huggingface/test_gh_secret', 'https://github.com/huggingface/hf_transfer', 'https://github.com/huggingface/ml-for-3d-course', 'https://github.com/huggingface/llm-swarm', 'https://github.com/huggingface/chug', 'https://github.com/huggingface/fuego', 'https://github.com/huggingface/competitions', 'https://github.com/huggingface/transfer-learning-conv-ai', 'https://github.com/huggingface/huggingface-sagemaker-snowflake-example', 'https://github.com/huggingface/safetensors', 'https://github.com/rust-lang/rust-analyzer', 'https://github.com/huggingface/discord-bots', 'https://github.com/llvm/llvm-zorg', 'https://github.com/pytorch/PiPPy', 'https://github.com/pytorch/tutorials', 'https://github.com/huggingface/transformers-bloom-inference', 'https://github.com/huggingface/optimum-neuron', 'https://github.com/pytorch/torchx', 'https://github.com/apache/arrow', 'https://github.com/huggingface/cosmopedia', 'https://github.com/huggingface/gym-aloha', 'https://github.com/huggingface/distil-whisper', 'https://github.com/huggingface/llm-vscode', 'https://github.com/huggingface/datatrove', 'https://github.com/huggingface/data-measurements-tool', 'https://github.com/huggingface/distill-bloom-deepspeed', 'https://github.com/cilium/ebpf', 'https://github.com/pytorch/ignite', 'https://github.com/pytorch/extension-cpp', 'https://github.com/huggingface/lerobot_hackathon_oct2024', 'https://github.com/pytorch/pytorch_sphinx_theme', 'https://github.com/pytorch/tvm', 'https://github.com/huggingface/trl', 'https://github.com/pytorch/add-annotations-github-action', 'https://github.com/huggingface/OBELICS', 'https://github.com/pytorch/java-demo', 'https://github.com/scipy/scipy', 'https://github.com/huggingface/datasets-viewer', 'https://github.com/huggingface/notebooks', 'https://github.com/huggingface/Unity-WebGL-template-for-Hugging-Face-Spaces', 'https://github.com/pytorch/ao', 'https://github.com/huggingface/education-toolkit', 'https://github.com/huggingface/datasets', 'https://github.com/huggingface/candle-paged-attention', 'https://github.com/huggingface/diffusers', 'https://github.com/huggingface/jat', 'https://github.com/etcd-io/etcd', 'https://github.com/huggingface/visual-blocks-custom-components', 'https://github.com/huggingface/optimum-graphcore', 'https://github.com/huggingface/optimum-tpu', 'https://github.com/huggingface/optimum-intel', 'https://github.com/huggingface/that_is_good_data', 'https://github.com/huggingface/transformers_bloom_parallel', 'https://github.com/huggingface/snapchat-lens-api', 'https://github.com/ray-project/ray', 'https://github.com/huggingface/text-embeddings-inference', 'https://github.com/huggingface/candle-rotary', 'https://github.com/huggingface/optimum-quanto', 'https://github.com/pytorch/xla', 'https://github.com/pytorch/extension-script', 'https://github.com/huggingface/candle-flash-attn-v1', 'https://github.com/huggingface/helm-common', 'https://github.com/huggingface/test-actions', 'https://github.com/huggingface/llm_training_handbook', 'https://github.com/huggingface/text-generation-inference-nix']
repos = []

owner_repos_str = ''
for url in owner_repos:
    owner = url.split('/')[-2]
    repo = url.split('/')[-1]
    repos.append({
        "owner": owner,
        "repo": repo
    })
    owner_repos_str += f" search_key__owner = '{owner}' and search_key__repo = '{repo}' or "

print(owner_repos_str)



ck_client = CKServer(host=clickhouse_server_info["HOST"],
                     port=clickhouse_server_info["PORT"],
                     user=clickhouse_server_info["USER"],
                     password=clickhouse_server_info["PASSWD"],
                     database=clickhouse_server_info["DATABASE"])
for owner_repo in repos:
    owner = owner_repo['owner']
    repo = owner_repo['repo']
    sql_ = f"""
insert into table commit_company
select *, toUnixTimestamp(now())
from (select search_key__owner,
             search_key__repo,
             author__login,
             commit__message,
             sha,
             commit__author__email,
             commit__author__date,
             company
      from (select *
            from github_commits
            where search_key__owner = '{owner}'
              and search_key__repo = '{repo}'
              and author__login global not in
                  (select robot_login_email from robot_login_email)
              and author__login != ''
              and length(parents.sha) = 1) as a global
               left join (select *
                          from (select login,
                                       company,
                                       min(start_at) as start_at,
                                       max(end_at)   as end_at
                                from (select a.*, b.company
                                      from (
                                               -- 一个login的一个邮箱后缀的起始时间和结束时间
                                               select login,
                                                      splitByChar('@', email)[2] as email_domain,
                                                      min(month)                 as start_at,
                                                      max(month)                 as end_at
                                               from (select commits.author_github_login as login,
                                                            commits.author_email        as email,
                                                            toInt64(concat(
                                                                    splitByChar('-', substring(`commits.author_date`, 1, 10))[1],
                                                                    splitByChar('-', substring(`commits.author_date`, 1, 10))[2]
                                                                --                                 ,
--                                     splitByChar('-', substring(`commits.author_date`, 1, 10))[3]
                                                                    ))                  as month
                                                     from nvidia_contributor_pr_v3 array join commits.author_github_login, commits.author_email, `commits.author_date`
                                                     where login != ''
                                                     union all
                                                     select author__login,
                                                            commit__author__email,
                                                            toYYYYMM(commit__author__date) as month
                                                     from github_commits
                                                     where author__login != ''
                                                       and author__login global not in
                                                           (select robot_login_email from robot_login_email)
                                                        )
                                               group by login, email_domain) as a global
                                               join company_email_map as b on a.email_domain = b.email_domain)
                                group by login, company)) as b on a.author__login = b.login
      where
        -- author_login为空值那就去掉
          toYYYYMM(commit__author__date) >= start_at
        and toYYYYMM(commit__author__date) <= end_at
      union all
      select search_key__owner,
             search_key__repo,
             author__login,
             commit__message,
             sha,
             commit__author__email,
             commit__author__date,
             company
      from (select *
            from github_commits
            where search_key__owner = '{owner}'
              and search_key__repo = '{repo}'
              and author__login global not in
                  (select robot_login_email from robot_login_email)
              and length(parents.sha) = 1
              and author__login = '') as a global
               join (select * from company_email_map) as b
                    on splitByChar('@', a.commit__author__email)[2] = b.email_domain
      union all
      select a.*, if(b.company = 'facebook', 'meta', b.company) as company
      from (select search_key__owner,
                   search_key__repo,
                   author__login,
                   commit__message,
                   sha,
                   commit__author__email,
                   commit__author__date
            from github_commits
            where search_key__owner = '{owner}'
              and search_key__repo = '{repo}'
              and author__login global not in
                  (select robot_login_email from robot_login_email)
              and length(parents.sha) = 1
              and sha global not in (select sha
                                     from (select author__login,
                                                  sha,
                                                  commit__author__email,
                                                  commit__author__date,
                                                  company
                                           from (select *
                                                 from github_commits
                                                 where search_key__owner = '{owner}'
                                                   and search_key__repo = '{repo}'
                                                   and author__login global not in
                                                       (select robot_login_email from robot_login_email)
                                                   and author__login != ''
                                                   and length(parents.sha) = 1) as a global
                                                    left join (select *
                                                               from (select login,
                                                                            company,
                                                                            min(start_at) as start_at,
                                                                            max(end_at)   as end_at
                                                                     from (select a.*, b.company
                                                                           from (
                                                                                    -- 一个login的一个邮箱后缀的起始时间和结束时间
                                                                                    select login,
                                                                                           splitByChar('@', email)[2] as email_domain,
                                                                                           min(month)                 as start_at,
                                                                                           max(month)                 as end_at
                                                                                    from (select commits.author_github_login as login,
                                                                                                 commits.author_email        as email,
                                                                                                 toInt64(concat(
                                                                                                         splitByChar('-', substring(`commits.author_date`, 1, 10))[1],
                                                                                                         splitByChar('-', substring(`commits.author_date`, 1, 10))[2]
                                                                                                     --                                 ,
--                                     splitByChar('-', substring(`commits.author_date`, 1, 10))[3]
                                                                                                         ))                  as month
                                                                                          from nvidia_contributor_pr_v3 array join commits.author_github_login, commits.author_email, `commits.author_date`
                                                                                          where login != ''
                                                                                          union all
                                                                                          select author__login,
                                                                                                 commit__author__email,
                                                                                                 toYYYYMM(commit__author__date) as month
                                                                                          from github_commits
                                                                                          where author__login != ''
                                                                                            and
                                                                                              author__login global not in
                                                                                              (select robot_login_email from robot_login_email)
                                                                                             )
                                                                                    group by login, email_domain) as a global
                                                                                    join company_email_map as b on a.email_domain = b.email_domain)
                                                                     group by login, company)) as b
                                                              on a.author__login = b.login
                                           where
                                             -- author_login为空值那就去掉
                                               toYYYYMM(commit__author__date) >= start_at
                                             and toYYYYMM(commit__author__date) <= end_at
                                           union all
                                           select author__login,
                                                  sha,
                                                  commit__author__email,
                                                  commit__author__date,
                                                  company
                                           from (select *
                                                 from github_commits
                                                 where search_key__owner = '{owner}'
                                                   and search_key__repo = '{repo}'
                                                   and author__login global not in
                                                       (select robot_login_email from robot_login_email)
                                                   and length(parents.sha) = 1
                                                   and author__login = '') as a global
                                                    join (select * from company_email_map) as b
                                                         on splitByChar('@', a.commit__author__email)[2] = b.email_domain)
                                     group by sha)) as a global
               left join (select a.*,
                                 b.company_commit_map                                as company_commit_map_by_all_commit,
                                 b.inferred_company_by_commit_count                  as inferred_company_by_all_commit_count,
                                 multiIf(inferred_company_by_commit_count = '' and profile_company = '' and
                                         inferred_company_by_all_commit_count != '',
                                         inferred_company_by_all_commit_count,
                                         inferred_company_by_commit_count != '', inferred_company_by_commit_count,
                                         inferred_company_by_commit_count = '' and
                                         inferred_company_by_all_commit_count != '',
                                         inferred_company_by_all_commit_count,
                                         inferred_company_by_commit_count = '' and
                                         inferred_company_by_all_commit_count = '' and
                                         profile_company != '', profile_company, '') as company
                          from (select a.*,
                                       final_company_inferred_from_company as profile_company
                                from (select author__login,
                                             groupArray((company, at_company_commit_count)) as company_commit_map
                                              ,
                                             if(length(company_commit_map) != 1 and company_commit_map[1].1 = '',
                                                company_commit_map[2].1,
                                                company_commit_map[1].1)                       inferred_company_by_commit_count
                                      from (select author__login, company, sum(commit_count) as at_company_commit_count
                                            from (select a.*, b.company
                                                  from (select author__login,
                                                               splitByChar('@', commit__author__email)[2] as email_domain,
                                                               count()                                    as commit_count
                                                        from (select author__login, sha, commit__author__email
                                                              from github_commits
                                                              where search_key__owner = '{owner}'
                                                                and search_key__repo = '{repo}'
                                                                and author__login global not in
                                                                    (select robot_login_email from robot_login_email)
                                                                and author__login != ''
                                                              group by author__login, sha, commit__author__email)
                                                        group by author__login, email_domain) as a global
                                                           left join (select * from company_email_map) as b on a.email_domain = b.email_domain)
                                            group by author__login, company
                                            order by author__login, at_company_commit_count desc)
                                      group by author__login) as a global
                                         left join (
                                    select login, final_company_inferred_from_company
                                    from github_profile
                                    where final_company_inferred_from_company != ''
                                    group by login, final_company_inferred_from_company
                                    ) as b on a.author__login = b.login) as a global
                                   left join (select author__login,
                                                     groupArray((company, at_company_commit_count)) as company_commit_map
                                                      ,
                                                     if(length(company_commit_map) != 1 and
                                                        company_commit_map[1].1 = '',
                                                        company_commit_map[2].1,
                                                        company_commit_map[1].1)                       inferred_company_by_commit_count
                                              from (select author__login,
                                                           company,
                                                           sum(commit_count) as at_company_commit_count
                                                    from (select a.*, b.company
                                                          from (select author__login,
                                                                       splitByChar('@', commit__author__email)[2] as email_domain,
                                                                       count()                                    as commit_count
                                                                from (select author__login, sha, commit__author__email
                                                                      from github_commits
                                                                      where author__login global not in
                                                                            (select robot_login_email from robot_login_email)
                                                                        and author__login != ''
                                                                      group by author__login, sha, commit__author__email)
                                                                group by author__login, email_domain) as a global
                                                                   left join (select * from company_email_map) as b on a.email_domain = b.email_domain)
                                                    group by author__login, company
                                                    order by author__login, at_company_commit_count desc)
                                              group by author__login) as b on a.author__login = b.author__login) as b
                         on a.author__login = b.author__login)
"""
    # ck_client.execute_no_params(sql_)
    # print(f'successful to insert commit_company_map {owner}:::{repo}')
