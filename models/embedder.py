import torch


# Positional encoding embedding. Code was taken from https://github.com/bmild/nerf.
class Embedder:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.create_embedding_fn()

    def create_embedding_fn(self):
        embed_fns = []
        d = self.kwargs['input_dims']
        out_dim = 0
        if self.kwargs['include_input']:
            embed_fns.append(lambda x: x)
            out_dim += d

        base_freq = self.kwargs['base_freq']
        base_freq = torch.tensor(base_freq).cuda()

        max_freq = self.kwargs['max_freq_log2']
        N_freqs = self.kwargs['num_freqs']

        if self.kwargs['log_sampling']:
            freqs = 2. ** torch.linspace(0., max_freq, N_freqs)
        else:
            freqs = torch.linspace(2.**0., 2.**max_freq, N_freqs)

        freqs = freqs.cuda()
        freqs = base_freq.view(1, -1) * freqs.cuda().view(-1, 1)

        for freq in freqs:
            for p_fn in self.kwargs['periodic_fns']:
                freq = freq.view(1, -1)
                embed_fns.append(lambda x, p_fn=p_fn, freq=freq: p_fn(x * freq))
                out_dim += d

        self.embed_fns = embed_fns
        self.out_dim = out_dim

    def embed(self, inputs):
        return torch.cat([fn(inputs) for fn in self.embed_fns], -1)


def get_embedder(multires, input_dims=3, base_freq=1.0):
    embed_kwargs = {
        'include_input': True,
        'input_dims': input_dims,
        'max_freq_log2': multires-1,
        'num_freqs': multires,
        'log_sampling': True,
        'periodic_fns': [torch.sin, torch.cos],
        'base_freq': base_freq
    }
    embedder_obj = Embedder(**embed_kwargs)
    def embed(x, eo=embedder_obj):
        return eo.embed(x)
    out_dim = embedder_obj.out_dim
    return embed, out_dim